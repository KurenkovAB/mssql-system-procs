USE [master]
GO
create or alter proc [dbo].[sp_tree_blocked]
as
set nocount on;

declare @process table
(
	spid smallint not null,
	kpid smallint not null,
	blocked smallint not null,
	lastwaittype nchar(32) not null,
	waitresource nchar(256) not null,
	dbname nvarchar(128) null,
	cpu int not null,
	physical_io bigint not null,
	memusage int not null,
	status nchar(30),
	loginame nchar(128) not null,
	login_time datetime not null,
	last_batch datetime not null,
	waittime bigint not null,
	hostname sysname,
	program_name nvarchar(256),
	text nvarchar(max)
);

insert @process
select
	p.spid,
	p.kpid,
	p.blocked,
	p.lastwaittype,
	p.waitresource,
	db_name(p.dbid) dbname,
	p.cpu,
	p.physical_io,
	p.memusage,
	p.status,
	p.loginame,
	p.login_time,
	p.last_batch,
	p.waittime,
	p.hostname,
	p.program_name,
	t.text
from sys.sysprocesses p
outer apply sys.dm_exec_sql_text(p.sql_handle) t;

with src as
(select *, rn=row_number() over (partition by spid order by kpid) from @process)
, cte as
(
	select
		*,
		0 as lev,
		convert(varchar(max),spid) as list
	from src
	where spid in (select blocked from src)
	and blocked=0

	union all

	select
		src.*,
		cte.lev+1,
		cte.list+' ;'+convert(varchar(max),src.spid)
	from src join cte
		on src.blocked = cte.spid
		and src.rn = cte.rn
)
select top 10000 replicate('.',lev*5)+convert(varchar,spid) as tree,
	spid,
	blocked,
	kpid,
	lastwaittype,
	dbname,
	cpu,
	physical_io,
	memusage,
	status,
	loginame,
	waittime,
	hostname,
	program_name,
	login_time,
	last_batch,
	waitresource,
	text
from cte
order by list, rn
option (maxrecursion 30000);
