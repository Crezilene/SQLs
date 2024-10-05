select
    distinct cf.order_id as ID,
    b.short_name as BANCO,
    cdb.code_bank as NUMERO,
    cdb.agency as AGEN,
    cdb.account_number as CONTA,
    case
        when cdb.account_type = 1 then 'Corrente'
        else 'Poupança'
    end CORRENTEPOUPANCA,
    cdb.cardholder_name as NOMECOMPLETO,
    cdb.cardholder_document CPFCNPJ,
    (
        select
            sum(cfi.total_refund)
        from
            tabela_items cfi
        where
            cfi.cancellation_flight_id = cf.id
    ) as VALORESTORNO,
    cf.total_refund as VALOR,
    cf.refund_order_id as ORConvencional,
    cf.refund_order_miles_id as ORMilhas,
    case
        when cf.refund_order_id is not null
        and cf.refund_order_miles_id is null then 'Convencional'
        when cf.refund_order_miles_id is not null
        and cf.refund_order_id is null then 'Outros'
        else 'Mesclado'
    end TIPOEDEESTORNO,
    cfs.description as STATUSOR,
    c.created_name as AGENTE,
    cr.description as MOTIVO,
    cfl.created_original_at as DATAULTIMAATUALIZACAO,
    cf.created_at as DATACRIACAOOR
from
    tabela_flights cf
    inner join cancellations c on cf.id = c.cancellable_id
    left join cancellation_data_bank cdb on cdb.cancellation_id = c.id
    and cdb.is_active = true
    left JOIN banks b on b.code_bank = cdb.code_bank
    inner join cancellation_flight_logs cfl on cfl.id = (
        select
            id
        from
            cancellation_flight_logs cfl2
        where
            cfl2.cancellation_flight_id = cf.id
        order by
            cfl2.created_original_at desc
        limit
            1
    )
    inner join cancellation_flight_statuses cfs on cfl.cancellation_flight_status_id = cfs.id
    inner join cancellation_reasons cr on cr.id = cf.reason_id

    -------------------------------------------------------------------------------------------------------

    SELECT 
	id,
	status 
    FROM db_silver.dts_123_financial_reports_refunds
union all
SELECT 
	id as id,
	status
FROM `banco`.tabela;