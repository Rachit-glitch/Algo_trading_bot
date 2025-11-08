from ib_insync import *
ib=IB(); ib.connect('127.0.0.1',7497,clientId=19)
dia=Stock('DIA','SMART','USD')
t=ib.reqMktData(dia,'',False,False); ib.sleep(2)
print('Last:', t.last, 'Bid:', t.bid, 'Ask:', t.ask)
ib.disconnect()
