"""Execute real model SQL on synthetic PostgreSQL 17 data. No live connections.

Run: python -m unittest orpheus_engine_tests.test_hackatime_category_hours
Docker must be available. The container has no published ports or persistent volumes.
"""
import json
from pathlib import Path
import re
import subprocess
import time
import unittest

ROOT = Path(__file__).resolve().parents[1]
MODELS = ROOT / 'orpheus_engine_dbt/models/hackatime_analytics'


def render(sql):
    sql = re.sub(r'{{\s*config\(.*?\)\s*}}', '', sql, flags=re.S)
    return re.sub(r"{{\s*source\('hackatime_raw', 'heartbeats'\)\s*}}", 'hackatime.heartbeats', sql)


class CategoryHours(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.container = subprocess.check_output([
            'docker', 'run', '-d', '--rm', '--network=none',
            '-e', 'POSTGRES_HOST_AUTH_METHOD=trust', 'postgres:17-alpine'
        ], text=True).strip()
        try:
            for _ in range(60):
                if subprocess.run(['docker', 'exec', cls.container, 'pg_isready', '-h', '127.0.0.1', '-U', 'postgres'],
                                  capture_output=True).returncode == 0:
                    break
                time.sleep(0.25)
            cls.sql('''CREATE SCHEMA hackatime;
                CREATE TABLE hackatime.heartbeats (
                    id bigint, user_id bigint, time double precision, category text,
                    project text, language text, editor text, entity text);
                CREATE TABLE hackatime.email_addresses (id bigint, user_id bigint, email text);
            ''')
            # Each user is synthetic. Include category transitions, duplicate timestamps,
            # NULL grain keys, first/zero seconds, long gaps, midnight, and time units.
            rows = []
            def beat(user, offset, category, project='fixture', unit=1):
                rows.append((len(rows)+1, user, (1793505600+offset)*unit,
                             category, project, 'fixture', 'fixture', '/fixture/file'))
            beat(1,0,'coding'); beat(1,60,'ai coding'); beat(1,120,'coding')
            beat(2,0,'ai coding'); beat(2,60,' AI Coding ')
            beat(3,0,'coding'); beat(3,60,'coding'); beat(3,60,'ai coding')
            # AI wins stable id ordering, coding has 0 tracked but positive legacy gap.
            beat(4,0,'coding','old'); beat(4,60,'ai coding','ai'); beat(4,60,'coding',None)
            beat(5,0,None); beat(5,900,''); beat(5,960,'timelapsing'); beat(5,1020,'unknown')
            beat(6,0,'coding'); beat(6,1,'coding')
            beat(7,3599,'coding'); beat(7,3601,'coding','next')
            beat(8,0,'coding',unit=1000); beat(8,60,'coding',unit=1000000)
            beat(11,0,'coding'); beat(11,60,'coding')
            rows[-1] = rows[-1][:5] + (None, None, None)
            rows[-2] = rows[-2][:5] + (None, None, None)
            beat(9,0,' Coding '); beat(9,60,'Coding')
            beat(None,0,'coding',None); beat(None,60,'coding',None)
            # More than 3600 attributed seconds in a DST repeated local hour.
            beat(10,0,'ai coding')
            for i,t in enumerate(range(3600,10801,120)):
                beat(10,t,'ai coding' if i % 2 == 0 else 'coding')
            def literal(v):
                return 'NULL' if v is None else ("'"+v.replace("'","''")+"'" if isinstance(v,str) else str(v))
            cls.sql('INSERT INTO hackatime.heartbeats VALUES '+','.join('('+','.join(map(literal,r))+')' for r in rows))
            for name in ['daily_activity','hourly_project_activity']:
                for prefix, path in [('old',ROOT/'orpheus_engine_tests/fixtures/hackatime'),('new',MODELS)]:
                    cls.sql(f'CREATE TABLE {prefix}_{name} AS '+render((path/(name+'.sql')).read_text()))
        except BaseException:
            cls.tearDownClass()
            raise

    @classmethod
    def tearDownClass(cls):
        subprocess.run(['docker','stop',cls.container],check=True,capture_output=True)

    @classmethod
    def sql(cls, sql):
        result = subprocess.run(['docker','exec','-i',cls.container,'psql','-h','127.0.0.1','-U','postgres',
                                 '-v','ON_ERROR_STOP=1','-At'],input=sql,text=True,capture_output=True)
        if result.returncode:
            raise AssertionError(result.stderr)
        return result.stdout.strip()

    def test_legacy_values_and_metadata(self):
        for name in ['daily_activity','hourly_project_activity']:
            columns = self.sql(f"SELECT string_agg(quote_ident(column_name), ', ' ORDER BY ordinal_position) FROM information_schema.columns WHERE table_name='old_{name}'")
            self.assertEqual(self.sql(f'''SELECT count(*) FROM (
                (SELECT {columns} FROM old_{name} EXCEPT ALL SELECT {columns} FROM new_{name} WHERE legacy_coding_row_present)
                UNION ALL
                (SELECT {columns} FROM new_{name} WHERE legacy_coding_row_present EXCEPT ALL SELECT {columns} FROM old_{name})
                ) x'''), '0')

    def test_compatibility_only(self):
        for name in ['daily_activity', 'hourly_project_activity']:
            self.assertEqual(self.sql(f'SELECT bool_and(legacy_coding_row_present) FROM new_{name}'), 't')
            self.assertEqual(self.sql(f'SELECT count(*) FROM new_{name}'),
                             self.sql(f'SELECT count(*) FROM old_{name}'))
            self.assertEqual(self.sql(f"SELECT count(*) FROM information_schema.columns WHERE table_name='new_{name}' AND column_name IN ('total_hours','ai_coding_hours','other_hours','tracked_coding_hours')"), '0')
        # Scheduled consumers must run against the pre-refresh schema too.
        for path in [
            'stardance_analytics/journey_user_stages.sql',
            'summer_2026_analytics/summer_unified_time_log.sql',
        ]:
            self.assertNotIn('legacy_coding_row_present', (MODELS.parent / path).read_text())


if __name__ == '__main__':
    unittest.main()
