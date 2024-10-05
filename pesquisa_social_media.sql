WITH tracking_order_social_media AS (
    SELECT
        fk_order_id,
        created_at,
        source,
        ROW_NUMBER() OVER (PARTITION BY fk_order_id ORDER BY created_at DESC) AS rn
    FROM banco.tabela
    WHERE created_at >= date_add('day', -30, current_date)
),
metasearch AS (
    SELECT
        iata_from AS iata_origem,
        iata_to AS iata_destino,
        COUNT(id) AS metasearch_count,
        external_user AS source,
        MAX(created_at) AS created_at
    FROM banco.tabela2
    WHERE browser_id = 'ExternalSearch'
        AND created_at >= date_add('day', -30, current_date)
    GROUP BY iata_from, iata_to, external_user
),
financial_reports AS (
    SELECT
        s.iata_from AS iata_origem,
        s.iata_to AS iata_destino,
        COUNT(s.id) OVER (PARTITION BY s.iata_from, s.iata_to) AS financial_count,
        o.fk_search_id,
        o.created_at,
        t.source,
        ROW_NUMBER() OVER (PARTITION BY s.iata_from, s.iata_to ORDER BY o.created_at DESC) AS rn
    FROM banco.tabela2 s
    INNER JOIN banco.tabela3 o ON s.id = o.fk_search_id
    INNER JOIN tracking_order_social_media t ON o.id = t.fk_order_id
    WHERE o.created_at >= date_add('day', -30, current_date)
        AND o.last_status IN (56, 60)
)
SELECT
    m.iata_origem,
    m.iata_destino,
    m.metasearch_count,
    COALESCE(f.financial_count, 0) AS financial_count,
    f.fk_search_id AS ultimo_fk_search_id,
    f.created_at AS ultimo_created_at,
    m.created_at AS metasearch_created_at,
    m.source
FROM metasearch m
LEFT JOIN (
    SELECT iata_origem, iata_destino, financial_count, fk_search_id, created_at, source
    FROM financial_reports
    WHERE rn = 1
) f ON m.iata_origem = f.iata_origem AND m.iata_destino = f.iata_destino AND m.source = f.source
ORDER BY m.source, m.iata_origem, m.iata_destino;
