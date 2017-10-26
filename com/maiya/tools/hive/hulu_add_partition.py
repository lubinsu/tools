# coding=utf-8
import datetime
import os

if __name__ == '__main__':
    tables = ['my_credit_hulu_application_check', 'my_credit_hulu_behavior_check', 'my_credit_hulu_cell_behavior',
              'my_credit_hulu_collection_contact', 'my_credit_hulu_common', 'my_credit_hulu_contact_list',
              'my_credit_hulu_contact_region', 'my_credit_hulu_data_source', 'my_credit_hulu_deliver_address',
              'my_credit_hulu_main_service', 'my_credit_hulu_person', 'my_credit_hulu_trip_consume',
              'my_credit_hulu_trip_info']

    delta = datetime.timedelta(days=1)
    begin = datetime.datetime.now() - delta
    end = datetime.datetime.now()

    with open('/opt/azkaban/workspace/streaming/tools/job/hive-alter.sql', 'w') as f:
        for table in tables:
            d = begin
            while d < end:
                f.write('alter table myd.%s drop partition (adddate=\'%s\');\n' % ( table, d.strftime("%Y%m%d")))
                f.write('alter table myd.%s add partition (adddate=\'%s\') location \'%s\';\n' % ( table, d.strftime("%Y%m%d"), d.strftime("%Y%m%d")))
                d += delta

    os.system('hive -f /opt/azkaban/workspace/streaming/tools/job/hive-alter.sql')
    os.system('impala-shell -f /opt/azkaban/workspace/streaming/tools/job/hive-invalidate.sql')
