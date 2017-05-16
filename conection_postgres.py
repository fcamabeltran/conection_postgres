#!/usr/bin/env python
#encoding:utf-8
import psycopg2
class Conexion(object):

  _db=None    
  def __init__(self, mhost, db, usr, pwd):
    self._db = psycopg2.connect(host=mhost, database=db, user=usr,  password=pwd)


  def manipular(self, sql):
    try:
      cur=self._db.cursor()
      cur.execute(sql)
      cur.close();
      self._db.commit()
    except:
      return False;
    return True;

  def consultar(self, sql):
    rs=None
    try:
      cur=self._db.cursor()
      cur.execute(sql)
      #fetchall()
      rs=cur.fetchone();
    except:
      return None
    return rs

  def proximaPK(self, tabla, key):
    sql='select max('+key+') from '+tabla
    rs = self.consultar(sql)
    pk = rs[0][0]  
    return pk+1

  def salir(self):
    self._db.close()

if __name__ == '__main__':
  con=Conexion('localhost','demo','postgres','postgres')
  sql ="insert into persona values(3,'felixcama',23,'ingeniero',99999999)"
  if(con.manipular(sql)):
    print('inserccion exitosa')

  rs=con.consultar("select * from persona")
  for x in rs:
    print(x)
  con.salir()


