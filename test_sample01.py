import sample01

sql_01 = """
        with w_1 as (
          select * from schema.phys_b
        )
        insert into schema.phys_a
        select * from w_1;
"""

sql_02 = """
        insert into schema.phys_b
        select *
        from schema.phys_c
        join schema.phys_d;
"""

sql_03 = """
        select *
        from schema.phys_c
        join schema.phys_d;
"""

sql_04 = """
        with w_1 as (
          with w_2 as (
            select * from schema.phys_b
          )
          select * from w_2
        )
        insert into schema.phys_a
        select * from w_1;
"""

sql_05 = """
        insert into schema.phys_a
        select * from schema.phys_b, schema.phys_c;
"""

sql_06 = """
        insert into schema.phys_a
        select * from (
          select * from schema.phys_b
        );
"""

sql_07 = """
        insert into schema.phys_a
        select * from (
          select * from (
            select * from schema.phys_b
          )
        );
"""

sql_08 = """
        select "ほげカラム", '!"#$%&''()-^[]@:;,./_=~|`{+*}<>?' from ほげテーブル
        where ほげカラム like 'あいう%'
          and ふがカラム = 'あいう%20えお';
"""

sql_09 = """
        insert into schema.phys_a (
          select * from schema.phys_b
        );
"""

sql_10 = """
        create table schema.phys_a as (
          select * from schema.phys_b
        );
"""

sql_11 = """
        create table schema.phys_a as (
          with w_1 as (
            select * from schema.phys_b
          )
          select * from w_1
        );
"""

sql_12 = """
        insert into schema.phys_a
        with w_1 as (
          select * from schema.phys_b
        )
        select * from w_1;
"""

sql_13 = """
        insert into schema.phys_a
        select *
        from schema.phys_b b
        join schema.phys_c c
          on true
        join schema.phys_d d
          on true
"""


def test_get_relations():
    ret = sample01.get_relations(sql_01)
    assert ret == [{'dst': ['schema.phys_a'], 'src': ['schema.phys_b']}]
    ret = sample01.get_relations(sql_02)
    assert ret == [
        {'dst': ['schema.phys_b'], 'src': ['schema.phys_c', 'schema.phys_d']}
    ]
    ret = sample01.get_relations(sql_03)
    assert ret == [{'dst': None, 'src': ['schema.phys_c', 'schema.phys_d']}]
    ret = sample01.get_relations(sql_04)
    assert ret == [{'dst': ['schema.phys_a'], 'src': ['schema.phys_b']}]
    ret = sample01.get_relations(sql_05)
    assert ret == [
        {'dst': ['schema.phys_a'], 'src': ['schema.phys_b', 'schema.phys_c']}
    ]
    ret = sample01.get_relations(sql_06)
    assert ret == [{'dst': ['schema.phys_a'], 'src': ['schema.phys_b']}]
    ret = sample01.get_relations(sql_07)
    assert ret == [{'dst': ['schema.phys_a'], 'src': ['schema.phys_b']}]
    ret = sample01.get_relations(sql_08)
    assert ret == [{'dst': None, 'src': ['ほげテーブル']}]
    #ret = sample01.get_relations(sql_09)
    #assert ret == [{'dst': ['schema.phys_a'], 'src': ['schema.phys_b']}]
    #ret = sample01.get_relations(sql_10)
    #assert ret == [{'dst': ['schema.phys_a'], 'src': ['schema.phys_b']}]
    #ret = sample01.get_relations(sql_11)
    #assert ret == [{'dst': ['schema.phys_a'], 'src': ['schema.phys_b']}]
    #ret = sample01.get_relations(sql_12)
    #assert ret == [{'dst': ['schema.phys_a'], 'src': ['schema.phys_b']}]
    #ret = sample01.get_relations(sql_13)
    #assert ret == [
        #{'dst': ['schema.phys_a'], 'src': ['schema.phys_b', 'schema.phys_c', 'schema.phys_d']}
    #]


ret = sample01.get_relations(sql_01)
ret = sample01.get_relations(sql_02)
ret = sample01.get_relations(sql_03)
ret = sample01.get_relations(sql_04)
ret = sample01.get_relations(sql_05)
ret = sample01.get_relations(sql_06)
ret = sample01.get_relations(sql_07)
ret = sample01.get_relations(sql_08)
#ret = sample01.get_relations(sql_09)
#ret = sample01.get_relations(sql_10)
#ret = sample01.get_relations(sql_11)
#ret = sample01.get_relations(sql_12)
#ret = sample01.get_relations(sql_13)
