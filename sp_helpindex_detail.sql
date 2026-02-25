use master
go
/*
    procedure sp_helpindex_detail: v 1.1
    date created: 2025-06-24
    By: Aleksey Kurenkov

    date modified v1.1: 2026-02-25
    changed: во входном параметре вместо одиночного объекта можно через запятую передавать список
             вместе с именем БД, то есть из разных БД можно смотреть индексы на указанные в списке
             таблицы, если БД не указана, то из текущей БД.
*/
create or alter proc [dbo].[sp_helpindex_detail]
    @objlist nvarchar(max)
as
    set nocount on;

    drop table if exists #indexes;
    create table #indexes
    (
        db_name                 nvarchar(128) not null,
        obj_name                nvarchar(255) not null,
        index_name              sysname,
        type                    int,
        is_unique               bit,
        is_primary_key          int,
        is_unique_constraint    bit,
        filter_definition       nvarchar(max),
        group_name              sysname,
        partition_count         int,
        used_page_count         bigint,
        reserved_page_count     bigint,
        row_count               bigint,
        user_updates            bigint,
        user_seeks              bigint,
        user_scans              bigint
        --primary key (db_name, obj_name, index_name)
    )

    drop table if exists #index_columns;
    create table #index_columns
    (
        db_name         nvarchar(128) not null,
        obj_name        nvarchar(255),
        index_name      sysname,
        index_columns   nvarchar(max) not null,
        include_columns nvarchar(max) null,
        primary key (db_name, obj_name, index_name)
    );

    declare crs cursor local fast_forward
    for select
            coalesce(parsename(value,3),db_name()),
            concat(parsename(value,2)+'.',parsename(value,1))
        from string_split(@objlist,',');

    open crs;

    declare @db         nvarchar(128),
            @obj        nvarchar(256),
            @query      nvarchar(max);

    while 1=1
    begin
        fetch next from crs into @db, @obj;

        if @@fetch_status != 0 break;

        set @query = N'use ['+@db+'];
        select
            db_name(),
            ''[''+object_schema_name(i.object_id)+''].[''+object_name(i.object_id)+'']'' as obj_name,
            ''[''+i.name+'']'' as index_name,
            i.[type],
            i.is_unique,
            i.is_primary_key,
            i.is_unique_constraint,
            i.filter_definition,
            isnull(''[''+fg.name+'']'', ''[''+ps.name+''] ([''+cc.name+''])'') as group_name,
            pst.partition_count,
            pst.used_page_count,
            pst.reserved_page_count,
            pst.row_count,
            ius.user_updates,
            ius.user_seeks,
            ius.user_scans
        from sys.indexes i
        left join sys.dm_db_index_usage_stats ius
            on ius.database_id = db_id()
            and ius.object_id = i.object_id
            and ius.index_id = i.index_id
        left join
        (
            select
                object_id,
                index_id,
                count(*) as partition_count,
                sum(used_page_count) as used_page_count,
                sum(reserved_page_count) as reserved_page_count,
                sum(row_count) as row_count
            from sys.dm_db_partition_stats
            group by
                object_id,
                index_id
        ) pst 
            on pst.object_id = i.object_id
            and pst.index_id = i.index_id
        left join sys.filegroups fg
            on fg.data_space_id = i.data_space_id
        left join sys.partition_schemes ps
            on i.data_space_id = ps.data_space_id
        left join sys.index_columns ic
            join sys.columns cc
                on ic.object_id = cc.object_id
                and ic.column_id = cc.column_id
            on ic.object_id = i.object_id
            and ic.index_id = i.index_id
            and ic.partition_ordinal = 1
        where   i.type > 0
            and (i.object_id = object_id(@obj) or object_id(@obj) is null)
        option(recompile)';

        insert into #indexes exec sp_executesql
            @query,
            N'@obj nvarchar(256)',
            @obj;

        set @query = N'use ['+@db+'];
        select
            db_name(),
            ''[''+object_schema_name(i.object_id)+''].[''+object_name(i.object_id)+'']'' as obj_name,
            ''[''+i.name+'']'' as index_name,
            string_agg((iif(ic.key_ordinal > 0, c.name + iif(ic.is_descending_key=1,''(-)'',''''), null)),'', '') within group (order by ic.index_column_id) ind_fld,
            string_agg((iif(ic.key_ordinal = 0, c.name + iif(ic.is_descending_key=1,''(-)'',''''), null)),'', '') within group (order by ic.index_column_id) inc_fld
        from sys.indexes i
        join sys.index_columns ic
            on ic.object_id = i.object_id
            and i.index_id = ic.index_id
        join sys.columns c
            on c.object_id = ic.object_id
            and c.column_id = ic.column_id
        where   i.type > 0
            and (i.object_id = object_id(@obj) or object_id(@obj) is null)
        group by
            i.object_id,
            i.name
        option(recompile)';

        insert into #index_columns exec sp_executesql
            @query,
            N'@obj nvarchar(256)',
            @obj;
    end;

    close crs;
    deallocate crs;

    select
        i.db_name,
        i.obj_name,
        i.index_name,
        iif(type=1,'clustered','nonclustered')+
        case
            when i.is_primary_key=1 then ', primary key'
            when i.is_unique_constraint=1 then ', unique key'
            when i.is_unique=1 then ', unique'
                else ''
        end as index_description,
        c.index_columns as index_keys,
        c.include_columns,
        i.filter_definition,
        i.group_name,
        i.row_count,
        i.partition_count,
        i.used_page_count,
        i.reserved_page_count,
        i.user_updates,
        i.user_seeks,
        i.user_scans
    from #index_columns c
    join #indexes i
        on i.obj_name = c.obj_name
        and i.index_name = c.index_name
    order by i.db_name, i.obj_name, i.type, i.index_name;
