# //////////////////////////////////////////////////////////////////////////////
# Place to test new implementations for pgbouncer unstable tests.
#
# Created by Postgres Professional.
#

import pytest
import time
import uuid

import psycopg
import pytest

from concurrent.futures import ThreadPoolExecutor

from .utils import (
    LINUX,
    USE_UNIX_SOCKETS,
    LIBPQ_SUPPORTS_PIPELINING,
    wait_until,
)

# //////////////////////////////////////////////////////////////////////////////


@pytest.fixture(
    params=[pytest.param(x, id="step_timeout_{}".format(x)) for x in range(3)]
)
def step_timeout(request: pytest.FixtureRequest) -> int:
    return request.param


# ------------------------------------------------------------------------
@pytest.mark.skipif("not LIBPQ_SUPPORTS_PIPELINING")
def test_evict_statement_cache_pipeline_failure_v2(bouncer, step_timeout: int):
    #
    # See: https://github.com/pgbouncer/pgbouncer/issues/1480
    #
    bouncer.admin(f"set max_prepared_statements=1")

    with bouncer.conn() as conn:
        # Phase 1: Breaking the Pipeline
        with conn.pipeline() as p:
            curs = [conn.cursor() for _ in range(4)]
            curs[0].execute("SELECT 1", prepare=True)
            time.sleep(step_timeout)

            with pytest.raises(psycopg.errors.SyntaxError):
                curs[1].execute("bad query", prepare=True)
                p.sync()

        # We CANNOT continue in this p block,
        # because it's marked as [BAD].
        # We'll exit it so psycopg can clean up.

        # BUT WE'LL STILL CHECK THE RESULT
        assert curs[0].fetchall() == [(1,)]

        # Phase 2: Check that the bouncer is alive and the statement cache is working
        # Create a NEW pipeline block on the same connection
        with conn.pipeline() as p:
            new_curs = conn.cursor()
            for _ in range(2):
                new_curs.execute("SELECT 1", prepare=True)
                p.sync()
                assert new_curs.fetchall() == [(1,)]


# //////////////////////////////////////////////////////////////////////////////


@pytest.fixture(
    params=[pytest.param(x, id="step_timeout_{}".format(x)) for x in range(3)]
)
def way4_timeout(request: pytest.FixtureRequest) -> int:
    return request.param


# ------------------------------------------------------------------------
@pytest.mark.skipif(
    "not USE_UNIX_SOCKETS", reason="This test does not apply to non unix sockets"
)
def test_shutdown_wait_for_servers_v2(bouncer, way4_timeout: int):
    """
    Test that after issuing `SHUTDOWN WAIT_FOR_SERVERS` pgbouncer
    is no longer accessible via 127.0.0.1 but is still accessible
    on UNIX socket until the last client leaves the pgbouncer instance.
    """
    assert type(way4_timeout) is int
    assert way4_timeout >= 0

    def LOCAL_sleep(step_id: str):
        print(
            "step [{}]. sleep {} sec(s)...".format(
                step_id,
                way4_timeout,
            )
        )
        time.sleep(way4_timeout)
        return

    socket_directory = bouncer.config_dir if LINUX else "/tmp"

    test_dbname = "user_passthrough2"
    test_user = "postgres"

    signal_table = "tbl_{}".format(uuid.uuid4().bytes.hex())

    print("test table is [{}]".format(signal_table))

    C_TASK_SQL_TEMPL = """DO $$
DECLARE
    row_exists boolean;
    counter int := 0;
BEGIN
    /* It is a signal "we are within server" in an autonomous transaction */
    PERFORM dblink_exec('{0}', 'INSERT INTO {1} (id) VALUES (1);');

    LOOP
        SELECT t.found INTO row_exists
        FROM dblink('{0}',
                    'SELECT EXISTS (SELECT 1 FROM {1} WHERE id = 1)')
        AS t(found boolean);

        IF NOT row_exists THEN
            EXIT;
        END IF;

        counter := counter + 1;
        IF counter >= 60 THEN
            RAISE EXCEPTION 'Polling timeout: signal record did not disappear';
        END IF;

        PERFORM pg_sleep(1);
    END LOOP;
END $$;"""

    with ThreadPoolExecutor(max_workers=100) as pool, bouncer.cur(
        autocommit=True, dbname=test_dbname, user=test_user
    ) as cur1, bouncer.admin_runner.cur():
        cur1.execute(
            "CREATE TABLE {} (id INTEGER NOT NULL PRIMARY KEY);".format(
                signal_table,
            )
        )
        cur1.execute("CREATE EXTENSION IF NOT EXISTS dblink SCHEMA public;")

        real_db = cur1.execute("SELECT current_database();").fetchall()[0][0]
        assert real_db == "p2"  # It is so!

        print("Run task1 on conn1")
        pg_cn_str = "host={} port={} dbname={} user={}".format(
            bouncer.pg.host,
            bouncer.pg.port,
            real_db,
            test_user,
        )
        task_sql = C_TASK_SQL_TEMPL.format(
            pg_cn_str,
            signal_table,
        )
        q1 = pool.submit(cur1.execute, task_sql)

        with bouncer.pg.cur(autocommit=True, dbname=real_db, user=test_user) as cur2:
            for _ in wait_until("Did not get signal from conn1", timeout=60):
                print("Waits for signal from conn1")
                r = cur2.execute(
                    "SELECT id FROM {} WHERE id=1;".format(signal_table)
                ).fetchall()
                if len(r) == 1:
                    break
                continue

        LOCAL_sleep("1")
        bouncer.admin("SHUTDOWN WAIT_FOR_SERVERS")
        LOCAL_sleep("2")
        with pytest.raises(psycopg.errors.OperationalError):
            bouncer.test(host=bouncer.config_dir)
        LOCAL_sleep("3")
        bouncer.admin("SHOW VERSION", host=socket_directory)
        LOCAL_sleep("4")
        with pytest.raises(psycopg.errors.OperationalError):
            bouncer.test(host="127.0.0.1")

        print("stop task1")
        with bouncer.pg.cur(
            autocommit=True,
            dbname=real_db,
            user=test_user,
        ) as cur2:
            cur2.execute("delete from {} where id=1;".format(signal_table))

        print("wait for task1")
        q1.result()
        print("task1 finished!")

    # Wait for janitor to close unix socket
    for _ in wait_until("Bouncer did not exit", timeout=60):
        try:
            bouncer.test(host=socket_directory)
        except psycopg.errors.OperationalError:
            break  # Success!
        continue


# //////////////////////////////////////////////////////////////////////////////


def test_cancel_race_v2(bouncer):
    # Make sure only one query can run at the same time so that its ensured
    # that both clients will use the same server connection.

    # Idea: we will use dblink and native SQL features to syncronization.

    # bouncer.admin("set default_pool_size=10")
    bouncer.admin("set server_idle_timeout=2")
    bouncer.admin("set verbose=1")

    conn1 = None
    conn2 = None

    test_dbname = "user_passthrough2"
    test_user = "postgres"

    try:
        cn0_str = "host={} port={} dbname={} user={}".format(
            bouncer.host,
            bouncer.port,
            test_dbname,
            test_user,
        )

        conn1 = bouncer.conn(dbname=test_dbname, user=test_user)
        cur1 = conn1.cursor()
        conn2 = bouncer.conn(dbname=test_dbname, user=test_user)
        cur2 = conn2.cursor()

        sql1 = """DO $$
BEGIN
    /* It locks conn2 */
    UPDATE test_cancel_race_v2 SET data='aaa' WHERE id=1;
    /* It is a signal "we are within server" in an autonomous transaction */
    PERFORM dblink_exec('{}', 'INSERT INTO test_cancel_race_v2 (id) VALUES (2);');
    /* Cancel signal is waited */
    PERFORM pg_sleep(60);
END $$;""".format(
            cn0_str
        )

        with ThreadPoolExecutor(max_workers=100) as pool:
            conn1.execute(
                "CREATE TABLE test_cancel_race_v2\n"
                "(id INTEGER NOT NULL PRIMARY KEY,\n"
                "data VARCHAR(32));"
            )
            conn1.execute("INSERT INTO test_cancel_race_v2 (id) VALUES (1);")
            conn1.execute("CREATE EXTENSION IF NOT EXISTS dblink SCHEMA public;")

            print("Run task1 on conn1")
            q1 = pool.submit(cur1.execute, sql1)

            for _ in wait_until(
                "Did not get signal from conn1", timeout=60, interval=0.2
            ):
                print("Waits for signal from conn1")
                r = cur2.execute(
                    "SELECT id FROM test_cancel_race_v2 WHERE id=2;"
                ).fetchall()
                if len(r) == 1:
                    break
                continue

            # It waits for conn1
            print("Run task2 on conn2")
            q2 = pool.submit(
                cur2.execute, "UPDATE test_cancel_race_v2 SET data='bbb' WHERE id=1;"
            )

            print("Run cancels")
            cancels = [pool.submit(conn1.cancel) for _ in range(100)]

            # Spam many concurrent cancel requests to try and with the goal of
            # triggering race conditions
            print("Wait for cancels")
            for c in cancels:
                c.result()

            print("Check task1")
            with pytest.raises(
                psycopg.errors.QueryCanceled, match="due to user request"
            ):
                q1.result()

            print("Check task2")
            q2.result()

            r = cur2.execute(
                "SELECT data FROM test_cancel_race_v2 WHERE id=1;"
            ).fetchall()
            assert r == [("bbb",)]

            bouncer.print_logs()
    finally:
        if conn1 is not None:
            conn1.close()
        if conn2 is not None:
            conn2.close()


# //////////////////////////////////////////////////////////////////////////////
