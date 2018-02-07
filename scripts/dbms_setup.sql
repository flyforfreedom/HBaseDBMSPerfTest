create table hbasePerfTest (
    id    char(6) casespecific,
    val   integer
) unique primary index (id);


insert into hbaseperftest values ('aa', '1234');
insert into hbaseperftest values ('Aa', '4567');

select *
from hbasePerfTest;
