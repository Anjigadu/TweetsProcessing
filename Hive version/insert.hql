insert into hiverawtags
select h1.time+${hiveconf:shifts}, h1.tag, 0 from hiverawtags h1 left join hiverawtags h2 on h1.tag = h2.tag and h1.time = h2.time + ${hiveconf:shifts} where h2.tag is null;
