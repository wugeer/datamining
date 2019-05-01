from datetime import datetime, date, timedelta

#def get_sale_day(item):
#    if int(str(item)[11:13]) < 3:
#        item = datetime.datetime.strptime(str(item)[:10], "%Y-%m-%d") + datetime.timedelta(days=-1)
#    return str(item)[:10]
item = (date.today() + timedelta(days=-1)).strftime("%Y-%m-%d")
item = '2018-11-05'


def test(*item):
    for i in item:
        print(i)
    print(item)

test(1,1,4)

