sql_drop_tables = '''
    DROP MATERIALIZED VIEW IF EXISTS contracts_groups_view;
    DROP TABLE IF EXISTS contracts;
    DROP TABLE IF EXISTS dim_budget;
    DROP TABLE IF EXISTS dim_okpd;
    DROP TABLE IF EXISTS dim_groups;
    DROP TABLE IF EXISTS dim_customers;
    DROP TABLE IF EXISTS dim_suppliers;
    DROP TABLE IF EXISTS dim_area;
'''

sql_create_tables = '''
    CREATE TABLE IF NOT EXISTS dim_budget
    (
        id integer NOT NULL,
        name character varying(100),
        CONSTRAINT dim_budget_pkey PRIMARY KEY (id)
    );

    CREATE TABLE IF NOT EXISTS dim_okpd (
        id SERIAL PRIMARY KEY,
        code varchar(15) NOT NULL,
        parent_code varchar(15) NULL,
        name varchar(512) NOT NULL,
        comment varchar(2048) NULL
    );
    
    CREATE TABLE IF NOT EXISTS dim_groups
    (
        id integer NOT NULL,
        name character varying(100),
        CONSTRAINT dim_groups_pkey PRIMARY KEY (id)
    );
    
    insert into dim_groups (id, name) values (1, 'Строительно-монтажные работы (СМР)');
    insert into dim_groups (id, name) values (2, 'Проектно-изыскательские работы (ПИР)');
    insert into dim_groups (id, name) values (3, 'Строительный надзор');
    insert into dim_groups (id, name) values (4, 'Подключение коммуникаций');
    insert into dim_groups (id, name) values (5, 'Прочее');
    
    CREATE TABLE IF NOT EXISTS dim_area
    (
        id integer NOT NULL,
        name character varying(1000),
        CONSTRAINT dim_area_pkey PRIMARY KEY (id)
    );    
        
    CREATE TABLE IF NOT EXISTS dim_customers
    (
        id integer NOT NULL,
        name character varying(1000),
        area character varying(1000),
        area_id integer,
        CONSTRAINT dim_customers_pkey PRIMARY KEY (id),
        CONSTRAINT "FK_CUSTOMERS_AREA_ID" FOREIGN KEY (area_id)
            REFERENCES dim_area (id) MATCH SIMPLE
            ON UPDATE NO ACTION
            ON DELETE NO ACTION
            NOT VALID
    );
    
    CREATE TABLE IF NOT EXISTS dim_suppliers
    (
        id integer NOT NULL,
        name character varying(1000),
        CONSTRAINT dim_suppliers_pkey PRIMARY KEY (id)
    );
'''

sql_drop_tables_contracts = '''
    DROP MATERIALIZED VIEW IF EXISTS contracts_groups_view;
    DROP TABLE IF EXISTS contracts;
'''

sql_create_tables_contracts = '''
    CREATE TABLE IF NOT EXISTS contracts (
        id INTEGER,
        reestrNumber varchar(25) NOT NULL,
        iczNumber varchar(40) NULL,
        inn varchar(15) NOT NULL,
        supplier TEXT NULL,
        code1 TEXT NULL,
        code2 TEXT NULL,
        customer TEXT NULL,
        customer_area TEXT NULL,
        code3 TEXT NULL,
        status_contract varchar(1000) NULL,
        description TEXT NULL,
        budget varchar(1024) NULL,
        contract_date TIMESTAMP NULL,
        date1 TEXT NULL,
        date2 TEXT NULL, 
        code4 TEXT NULL, 
        code5 TEXT NULL,
        contract_execution_date TIMESTAMP NULL,
        contract_end_date TIMESTAMP NULL,
        amount decimal NULL,
        contract_price decimal NULL,
        code6 TEXT NULL,
        code7 TEXT NULL,
        code8 TEXT NULL,
        okpd_name TEXT NULL,
        okpd_code varchar(250) NULL,
        group_id INTEGER NULL,
        okpd_id INTEGER NULL,
        customer_id INTEGER NULL,
        supplier_id INTEGER NULL,
        budget_id INTEGER NULL,
        contract_days INTEGER NULL,
        contract_year INTEGER NULL
    );
    
    CREATE INDEX idx_contracts_nulls_low ON contracts (okpd_code NULLS FIRST);
    CREATE INDEX idx_contracts_groups_nulls_low ON contracts (group_id NULLS FIRST);
    
    ALTER TABLE IF EXISTS contracts
        ADD CONSTRAINT "FK_CONTRACTS_GROUP_ID" FOREIGN KEY (group_id)
        REFERENCES dim_groups (id) MATCH SIMPLE
        ON UPDATE NO ACTION
        ON DELETE NO ACTION
        NOT VALID;
        
    ALTER TABLE IF EXISTS contracts
        ADD CONSTRAINT "FK_OKPD_ID" FOREIGN KEY (okpd_id)
        REFERENCES dim_okpd (id) MATCH SIMPLE
        ON UPDATE NO ACTION
        ON DELETE NO ACTION
        NOT VALID;

    ALTER TABLE IF EXISTS contracts
        ADD CONSTRAINT "FK_CONTRACTS_CUSTOMER_ID" FOREIGN KEY (customer_id)
        REFERENCES dim_customers (id) MATCH SIMPLE
        ON UPDATE NO ACTION
        ON DELETE NO ACTION
        NOT VALID;
        
    ALTER TABLE IF EXISTS contracts
        ADD CONSTRAINT "FK_CONTRACTS_SUPPLIER_ID" FOREIGN KEY (supplier_id)
        REFERENCES dim_suppliers (id) MATCH SIMPLE
        ON UPDATE NO ACTION
        ON DELETE NO ACTION
        NOT VALID;
        
        CREATE MATERIALIZED VIEW IF NOT EXISTS contracts_groups_view
        as
        select c.customer Customer, c.supplier Supplier, c.contract_date ContractDate, c.contract_end_date ContractEndDate, 
            c.contract_price ContractPrice, g.name GroupName 
        from contracts c
        inner join dim_groups g on g.id = c.group_id
        where c.group_id is not null
        WITH NO DATA;
        
        REFRESH MATERIALIZED VIEW contracts_groups_view;
'''

sql_scripts_update = '''
    update contracts c
        set group_id = 2
    where c.okpd_code like '41.1%';
    
    update contracts c
        set group_id = 2
    where c.okpd_code like '71.1%';
    
    update contracts c
        set group_id = 3
    where c.okpd_code like '43.22%';       
    
    update contracts c
        set group_id = 1
    where c.okpd_code like '41%'
        and c.group_id is null;
        
    update contracts c
        set group_id = 1
    where c.okpd_code like '42%'
        and c.group_id is null;
        
    update contracts c
        set group_id = 1
    where c.okpd_code like '43%'
        and c.group_id is null;
        
    update public.contracts c
        set group_id = 5
    where c.okpd_code = '42.2'
        and c.group_id = 1;        
        
    update public.contracts c
        set group_id = 5
    where c.okpd_code = '42.9'
        and c.group_id = 1;        
        
    update public.contracts c
        set group_id = 5
    where c.okpd_code = '43.2'
        and c.group_id = 1;   
        
    update public.contracts c
        set group_id = 5
    where c.okpd_code = '42.1'
        and c.group_id = 1;
        
    update public.contracts c
        set group_id = 5
    where c.okpd_code = '42.9'
        and c.group_id = 1;
        
    update public.contracts c
        set group_id = 3
    where c.okpd_code = '41.2'
        and c.group_id = 1;
                
    update public.contracts c
        set group_id = 2
    where c.okpd_code = '42.2'
        and c.description like '%изыск%';        
        
    update public.contracts c
        set group_id = 2
    where c.okpd_code = '42.2'
        and c.description like '%проект%';

    update public.contracts c
        set group_id = 2
    where c.okpd_code = '42.2'
        and c.description like '%изыск%';        
        
    update public.contracts c
        set group_id = 2
    where c.okpd_code = '43.9'
        and c.description like '%проект%';        
        
    update public.contracts c
        set group_id = 2
    where c.okpd_code = '43.9'
        and c.description like '%изыск%';            
        
    update public.contracts c
        set group_id = 4
    where c.okpd_code = '43.2'
        and c.description like '%подключение%';     

    update public.contracts c
        set group_id = 4
    where c.okpd_code = '42.2'
        and c.description like '%подключение%';         
        
    update public.contracts c
        set group_id = 3
    where c.okpd_code = '71.1'
        and c.description like '%контрол%';        
        
    update public.contracts c
        set group_id = 3
    where c.okpd_code = '71.1'
        and c.description like '%реконструкция%';
        
    update public.contracts c
        set group_id = 1
    where c.okpd_code = '71.1'
        and c.description like '%строительство%';                  

    update public.contracts c
        set contract_year = date_part('year', contract_date)
    where c.group_id is not null;

    update contracts c
        set contract_days = DATE_PART('day', contract_end_date - contract_date)
    where c.group_id is not null;
'''