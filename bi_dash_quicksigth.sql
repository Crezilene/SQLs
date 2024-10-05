
   SELECT 
   DATE(hps.created_at) AS Data,
   COUNT(hps.id) AS Pesquisas,
   hps.destination_id,
   DATE(hps.checkin) as Checkin,
   ps.ap
FROM
    tabela hps 
        LEFT JOIN
    tabela2 ps ON hps.fk_purchase_searchs_id = ps.id 
WHERE
	hps.id >= (SELECT MAX(id) FROM tabela2) - @Linhas
    and hps.created_at BETWEEN @StartDate and @EndDate
and hps.checkin >= now()
GROUP BY date(hps.created_at), hps.checkin, hps.destination_id",
		[StartDate = RangeStart, EndDate = RangeEnd, Linhas = Linhas]
    )