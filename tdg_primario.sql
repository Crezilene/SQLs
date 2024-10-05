WITH principal AS (

	with ac as (
        with a as (
     		SELECT
            y.data_resolucao  data_resolucao
            ,y.matricula             
            ,y.quantidade_interacoes
            ,y.quantidade_incidentes
            ,case when y.fila = 'Cancelamento/remarcação' then 1 WHEN y.fila = 'Remarcação ' THEN 1 else 0.7 END Multiplicadora
            ,y.quantidade_incidentes * (case when y.fila = 'Cancelamento/remarcação' then 1 WHEN y.fila = 'Remarcação ' THEN 1 else 0.7 END) QTD
            ,case when  y.quantidade_interacoes = 1 then 1
                  when y.quantidade_interacoes = 2 then 0.5 else 0.1 end  Multiplicador
            ,(y.quantidade_incidentes * (case when y.fila = 'Cancelamento/remarcação' then 1 WHEN y.fila = 'Remarcação ' THEN 1 else 0.7 END))  *
            (case when  y.quantidade_interacoes = 1 then 1
                  when y.quantidade_interacoes = 2 then 0.5
                  when y.quantidade_interacoes is null then 0.1
                  else 0.1 end) QTD            
            ,y.fila
            ,x.matricula_supervisor 
            ,x.matricula_coordenador
            ,x.matricula_gerente
            from banco.tabela y
            JOIN (SELECT * from banco.tabela where upper(funcao) = 'E-MAIL' AND upper(area) = 'RELACIONAMENTO PRIMÁRIO') x on x.matricula = y.matricula and month( y.data_resolucao ) = month(x.mes_referencia) and year( y.data_resolucao ) = year(x.mes_referencia)
        )
		, b AS (
            SELECT 
            data_resolucao
            ,matricula
            ,sum((quantidade_incidentes * (case when fila = 'Cancelamento/remarcação' then 1 WHEN fila = 'Remarcação ' THEN 1 else 0.7 END))  *
                            (case WHEN quantidade_interacoes = 1 then 1
                                  when quantidade_interacoes = 2 then 0.5 else 0.1 end))  / sum(quantidade_incidentes) Percent_Resolucao
            ,sum((quantidade_incidentes * (case when fila = 'Cancelamento/remarcação' then 1 WHEN fila = 'Remarcação ' THEN 1 else 0.7 END))  *
                            (case when  quantidade_interacoes = 1 then 1
                                  when quantidade_interacoes = 2 then 0.5 else 0.1 end)) Resolvidos                        
            ,matricula_supervisor 
            ,matricula_coordenador
            ,matricula_gerente
            from a
            group by
            data_resolucao
            ,matricula
            , matricula_supervisor 
            ,matricula_coordenador
            ,matricula_gerente
        )
		select data_resolucao
		,matricula
		,case when Percent_Resolucao > 1 then 0 else Percent_Resolucao end Percent_Resolucao
		,Resolvidos
		,case when Percent_Resolucao < m_bronze then (Percent_Resolucao - m_alerta)/(m_bronze - m_alerta)*(p_bronze - p_alerta)+p_alerta
		      when Percent_Resolucao < m_prata then (Percent_Resolucao - m_bronze)/(m_prata - m_bronze)*(p_prata - p_bronze)+p_bronze
		      when Percent_Resolucao < m_ouro then  (Percent_Resolucao - m_prata)/(m_ouro - m_prata)*(p_ouro - p_prata)+p_prata
		      when Percent_Resolucao < m_1 then  (Percent_Resolucao - m_ouro)/(m_1 - m_ouro)*(p_1 - p_ouro)+p_ouro 
		      when Percent_Resolucao < m_2 then  (Percent_Resolucao - m_1)/(m_2 - m_1)*(p_2 - p_1)+p_1      
		      else p_2 end p_resolutividade
		,matricula_supervisor 
		,matricula_coordenador
		,matricula_gerente
		from b
		LEFT JOIN banco.tabela w on month(b.data_resolucao) = month(w.ref) and year(b.data_resolucao) = year(w.ref)
		where LOWER(w.indicador) = 'resolutividade'  and lower(rtrim(ltrim(w.tipo))) = 'individual' and UPPER(w.origem) = 'PRIMARIO' 
    )
	,c as (
    -- Media dia do Percentual do Funcionario           
    -- Media mes media(Media dia do Percentual do Funcionario) ou seja media da media
                SELECT y.data_
                ,y.matricula
                ,x.area area_gabi
                ,sum(case when y.fila = 'Cancelamento/remarcação' then 0.3
                when y.fila = 'Pendências Reembolso' then 0.3 else 1 end ) recorrencia
                FROM (
	                select cast(data_ultima_criacao as date) data_, matricula,fila,area 
	                from banco.tabela
	                union all
	                select cast(data_respondido_cliente as date) data_, matricula, fila,area
	                from banco.tabela
				) y
                left join (SELECT * from banco.tabela where upper(funcao) = 'E-MAIL' AND upper(area) = 'RELACIONAMENTO PRIMÁRIO'
                          ) x on x.matricula = y.matricula  and month(y.data_) = month(x.mes_referencia) and year(y.data_) = year(x.mes_referencia)
                group by y.data_
                ,y.matricula
                ,x.area
            )
  ,cr as (
                select 
                 data_resolucao data_
                ,matricula
                ,sum(quantidade_incidentes) resolvidos_rec
                FROM banco.tabela                                                                    
                where atendente is not null
                group by
				1,2
           	)

,e as (
	WITH base_produtividade AS ( -- Cálculo da Produtividade com condicional de nulo NO dia de falta
		SELECT *
    	FROM (
	    	SELECT
	    	*
	    	,CASE WHEN Producao IS NULL THEN 0.1 ELSE Producao END Producao_aux
		    ,ROW_NUMBER() OVER(PARTITION BY matricula, data_resolucao ORDER BY data_resolucao DESC, CASE WHEN Producao IS NULL THEN 0.1 ELSE Producao END ASC) teste
	    	FROM (
			    SELECT
			    v.data_resolucao AS data_resolucao
		    	,v.matricula ,x.area,sum(v.quantidade_incidentes) Producao
			    ,case
			        when x.horario_inicio in ('00:00:00','01:00:00') then 'madrugada'
			        when x.horario_inicio in ('06:00:00') and x.horario_fim  = '15:00:00' then 'comercial'
			        else 'dia' end as escala
			    from banco.tabela v
			    JOIN (SELECT * from banco.tabela where upper(funcao) = 'E-MAIL' AND upper(area) = 'RELACIONAMENTO PRIMÁRIO') x on v.matricula = x.matricula and month( v.data_resolucao) = month(x.mes_referencia) and year(v.data_resolucao) = year(x.mes_referencia)
			    group by
			    1,2,3,5
			    UNION
			    SELECT
			    ass.data_ref data_resolucao
			    ,ass.matricula
			    ,'FALTA' area
			    ,CASE WHEN ass.falta_injustificada >=1 THEN 0
				 WHEN ass.faltas_outros >=1 THEN NULL
				 WHEN ass.faltas_justificadas >= 1 THEN NULL END Producao
				,'dia' escala
			    FROM banco.tabela ass
			    JOIN (SELECT * from banco.tabela where upper(funcao) = 'E-MAIL' AND upper(area) = 'RELACIONAMENTO PRIMÁRIO') xx on ass.matricula = xx.matricula and month(xx.mes_referencia) = month (ass.data_ref) and year(xx.mes_referencia) = year (ass.data_ref)
			    )
		    )
		    WHERE teste = 1
)

	select
    data_resolucao
    ,matricula
    ,Producao
    ,case
        when Producao < m_bronze then (Producao - m_alerta)/(m_bronze - m_alerta)*(p_bronze - p_alerta)+p_alerta
        when Producao < m_prata then (Producao - m_bronze)/(m_prata - m_bronze)*(p_prata - p_bronze)+p_bronze
        when Producao < m_ouro then (Producao - m_prata)/(m_ouro - m_prata)*(p_ouro - p_prata)+p_prata
        when Producao < m_1 then (Producao - m_ouro)/(m_1 - m_ouro)*(p_1 - p_ouro)+p_ouro
        when Producao < m_2 then (Producao - m_1)/(m_2 - m_1)*(p_2 - p_1)+p_1     
        when Producao > m_2 then (Producao - m_2 ) * (p_2/m_2) + p_2 
        end as p_produtividade
    FROM base_produtividade
    LEFT JOIN banco.tabela m ON month(m.ref) = month(data_resolucao) and year(m.ref) = year(data_resolucao)
    WHERE lower(indicador) = 'produtividade' and escala = 'dia' and lower(rtrim(ltrim(m.tipo))) = 'individual' and UPPER(m.origem) = 'PRIMARIO'
    UNION
    SELECT
    data_resolucao
    ,matricula
    ,Producao
    ,case
        when Producao < m_bronze then (Producao - m_alerta)/(m_bronze - m_alerta)*(p_bronze - p_alerta)+p_alerta
        when Producao < m_prata then (Producao - m_bronze)/(m_prata - m_bronze)*(p_prata - p_bronze)+p_bronze
        when Producao < m_ouro then (Producao - m_prata)/(m_ouro - m_prata)*(p_ouro - p_prata)+p_prata
        when Producao < m_1 then (Producao - m_ouro)/(m_1 - m_ouro)*(p_1 - p_ouro)+p_ouro
        when Producao < m_2 then (Producao - m_1)/(m_2 - m_1)*(p_2 - p_1)+p_1     
        when Producao > m_2 then (Producao - m_2 ) * (p_2/m_2) + p_2 
        end as p_produtividade 
    FROM base_produtividade
    LEFT JOIN banco.tabela m ON month(m.ref) = month(data_resolucao) and year(m.ref) = year(data_resolucao)
    WHERE lower(indicador) = 'produtividade_madrugada' and escala = 'madrugada' and lower(rtrim(ltrim(m.tipo))) = 'individual' and UPPER(m.origem) = 'PRIMARIO' 
    UNION
    SELECT
    data_resolucao
    ,matricula
    ,Producao
    ,case
        when Producao < m_bronze then (Producao - m_alerta)/(m_bronze - m_alerta)*(p_bronze - p_alerta)+p_alerta
        when Producao < m_prata then (Producao - m_bronze)/(m_prata - m_bronze)*(p_prata - p_bronze)+p_bronze
        when Producao < m_ouro then (Producao - m_prata)/(m_ouro - m_prata)*(p_ouro - p_prata)+p_prata
        when Producao < m_1 then (Producao - m_ouro)/(m_1 - m_ouro)*(p_1 - p_ouro)+p_ouro
        when Producao < m_2 then (Producao - m_1)/(m_2 - m_1)*(p_2 - p_1)+p_1     
        when Producao > m_2 then (Producao - m_2 ) * (p_2/m_2) + p_2 
        end as p_produtividade 
    FROM base_produtividade
    LEFT JOIN banco.tabela m ON month(m.ref) = month(data_resolucao) and year(m.ref) = year(data_resolucao)
    where lower(indicador) = 'produtividade_comercial' and escala = 'comercial' and lower(rtrim(ltrim(m.tipo))) = 'individual' and UPPER(m.origem) = 'PRIMARIO'
)

, f as (
    SELECT
    cast(data_ref as date) AS data_ref
    ,matricula 
    ,faltas_justificadas                      
    FROM banco.tabela         
)
, g as (
    SELECT
    cast(data_ref as date) AS data_ref
    ,matricula            
    ,falta_injustificada          
    from banco.tabela         
)

, i as (
	SELECT
	cast(data_ref as date) data_ref
	,matricula            
	,faltas_outros            
	FROM banco.tabela        
)

,tabela as(
     SELECT
	 CASE WHEN CAST(ab.data_resolucao AS DATE) >= CAST('2022-07-01' AS DATE)
     THEN 
        CASE WHEN DAY(ab.data_resolucao) = 26 AND MONTH(ab.data_resolucao) = 12 THEN CAST(CONCAT(CAST(YEAR(ab.data_resolucao + INTERVAL '1' YEAR) AS varchar),'-',CAST(MONTH(ab.data_resolucao + INTERVAL '1' MONTH) AS varchar),'-','01') AS DATE)
             WHEN DAY(ab.data_resolucao) = 27 AND MONTH(ab.data_resolucao) = 12 THEN CAST(CONCAT(CAST(YEAR(ab.data_resolucao + INTERVAL '1' YEAR) AS varchar),'-',CAST(MONTH(ab.data_resolucao + INTERVAL '1' MONTH) AS varchar),'-','02') AS DATE)
             WHEN DAY(ab.data_resolucao) = 28 AND MONTH(ab.data_resolucao) = 12 THEN CAST(CONCAT(CAST(YEAR(ab.data_resolucao + INTERVAL '1' YEAR) AS varchar),'-',CAST(MONTH(ab.data_resolucao + INTERVAL '1' MONTH) AS varchar),'-','03') AS DATE)
             WHEN DAY(ab.data_resolucao) = 29 AND MONTH(ab.data_resolucao) = 12 THEN CAST(CONCAT(CAST(YEAR(ab.data_resolucao + INTERVAL '1' YEAR) AS varchar),'-',CAST(MONTH(ab.data_resolucao + INTERVAL '1' MONTH) AS varchar),'-','04') AS DATE)
             WHEN DAY(ab.data_resolucao) = 30 AND MONTH(ab.data_resolucao) = 12 THEN CAST(CONCAT(CAST(YEAR(ab.data_resolucao + INTERVAL '1' YEAR) AS varchar),'-',CAST(MONTH(ab.data_resolucao + INTERVAL '1' MONTH) AS varchar),'-','05') AS DATE)
             WHEN DAY(ab.data_resolucao) = 31 AND MONTH(ab.data_resolucao) = 12 THEN CAST(CONCAT(CAST(YEAR(ab.data_resolucao + INTERVAL '1' YEAR) AS varchar),'-',CAST(MONTH(ab.data_resolucao + INTERVAL '1' MONTH) AS varchar),'-','06') AS DATE)
        ELSE
            CASE WHEN DAY(ab.data_resolucao) = 26 THEN CAST(CONCAT(CAST(YEAR(ab.data_resolucao) AS varchar),'-',CAST(MONTH(ab.data_resolucao + INTERVAL '1' MONTH) AS varchar),'-','01') AS DATE)
                 WHEN DAY(ab.data_resolucao) = 27 THEN CAST(CONCAT(CAST(YEAR(ab.data_resolucao) AS varchar),'-',CAST(MONTH(ab.data_resolucao + INTERVAL '1' MONTH) AS varchar),'-','02') AS DATE)
                 WHEN DAY(ab.data_resolucao) = 28 THEN CAST(CONCAT(CAST(YEAR(ab.data_resolucao) AS varchar),'-',CAST(MONTH(ab.data_resolucao + INTERVAL '1' MONTH) AS varchar),'-','03') AS DATE)
                 WHEN DAY(ab.data_resolucao) = 29 THEN CAST(CONCAT(CAST(YEAR(ab.data_resolucao) AS varchar),'-',CAST(MONTH(ab.data_resolucao + INTERVAL '1' MONTH) AS varchar),'-','04') AS DATE)
                 WHEN DAY(ab.data_resolucao) = 30 THEN CAST(CONCAT(CAST(YEAR(ab.data_resolucao) AS varchar),'-',CAST(MONTH(ab.data_resolucao + INTERVAL '1' MONTH) AS varchar),'-','05') AS DATE)
                 WHEN DAY(ab.data_resolucao) = 31 THEN CAST(CONCAT(CAST(YEAR(ab.data_resolucao) AS varchar),'-',CAST(MONTH(ab.data_resolucao + INTERVAL '1' MONTH) AS varchar),'-','06') AS DATE)
            ELSE ab.data_resolucao  END 
        END     
	ELSE ab.data_resolucao END data
    ,ab.matricula
    ,ac.Percent_Resolucao
    ,ac.Resolvidos
    ,ac.p_resolutividade
    ,c.Recorrencia
    ,cr.resolvidos_rec
    ,e.Producao
    ,e.p_produtividade
    ,case when g.falta_injustificada >= 1 then 0 else 0  end  Assiduidade             
    ,now() ultima_atualizacao
    from (SELECT DISTINCT CAST(created AS date) data_resolucao, x.matricula FROM banco.tabela c
    JOIN (SELECT * from banco.tabela  where upper(funcao) = 'E-MAIL' AND upper(area) = 'RELACIONAMENTO PRIMÁRIO') x on MONTH(c.created ) = MONTH(x.mes_referencia)  AND  YEAR( c.created) = YEAR(x.mes_referencia)) ab
    left join ac on ab.matricula = ac.matricula and ab.data_resolucao = ac.data_resolucao 
    left join c on ab.matricula = c.matricula and ab.data_resolucao = c.data_
    left join cr on ab.matricula = cr.matricula and ab.data_resolucao = cr.data_
    left join e on ab.matricula = e.matricula and ab.data_resolucao = e.data_resolucao
    left join f on ab.matricula = cast(f.matricula as double) and ab.data_resolucao = f.data_ref
    left join g on ab.matricula = cast(g.matricula as double) and ab.data_resolucao = g.data_ref
    left join i on ab.matricula = cast(i.matricula as double) and ab.data_resolucao = i.data_ref
    where ab.data_resolucao >= cast('2021-08-01' as date)
)

,C2 AS (
	 with o as (
        SELECT month(data_posicao ) Mes_ref
        ,year(data_posicao ) Ano_ref
        ,case when area = 'Atendimento Primário' then 'Relacionamento Primário'
              when area = 'Atendimento Especializado' then 'Relacionamento Especializado' else area end area                       
        ,avg(case when is_nan(round(1.0*qtde_respondido_dentro_prazo/ (qtde_total - qtde_nao_respondido_fora_prazo_resolvido),2) ) then 0 else round(1.0*qtde_respondido_dentro_prazo/ (qtde_total - qtde_nao_respondido_fora_prazo_resolvido),2)  end) Percent
        FROM banco.tabela 
        where CAST(data_posicao AS date) >= CAST('2021-08-01' AS date) and area in ('Atendimento Primário', 'Atendimento Especializado') AND fila NOT IN ('Hotel','Pacote')
        group by  1,2,3
     )
    select
    o.Mes_ref
    ,o.Ano_ref
    ,o.Percent valor_ciclo_c
    ,case when o.Percent < m_bronze then (o.Percent - m_alerta)/(m_bronze - m_alerta)*(p_bronze - p_alerta)+p_alerta
              when o.Percent < m_prata then (o.Percent - m_bronze)/(m_prata - m_bronze)*(p_prata - p_bronze)+p_bronze
              when o.Percent < m_ouro then  (o.Percent - m_prata)/(m_ouro - m_prata)*(p_ouro - p_prata)+p_prata
              when o.Percent < m_1 then  (o.Percent - m_ouro)/(m_1 - m_ouro)*(p_1 - p_ouro)+p_ouro 
              when o.Percent < m_2 then  (o.Percent - m_1)/(m_2 - m_1)*(p_2 - p_1)+p_1      
              else p_2 end p_ciclo
    ,area
    ,w.tipo
    ,w.origem
    from o
    left join banco.tabela w on o.Mes_ref = month(w.ref) and o.Ano_ref = year(w.ref)
    where  lower(w.indicador) = 'ciclo 10 dias' AND lower(w.tipo) = 'coletivo' AND  UPPER(w.origem) = 'PRIMARIO' AND lower(o.area) = 'relacionamento primário'
)

, C5 AS (
    -- Media Acertos por Area.
	with o as (
	        select 
	        ano_ref
	        ,mes_ref
	        ,avg(d.nota_conhecimento) AS Acertos
	        from banco.tabela d
	        JOIN (SELECT * from banco.tabela  where upper(funcao) = 'E-MAIL' AND upper(area) = 'RELACIONAMENTO PRIMÁRIO') e ON d.matricula = e.matricula AND d.ano_ref = YEAR(e.mes_referencia) AND d.mes_ref = MONTH(e.mes_referencia)
	        group by 
	        ano_ref
	        ,mes_ref 
        
         	  )
	select
	o.Mes_ref
	,o.Ano_ref
	,o.Acertos valor_conhec_c
	,case when o.Acertos < m_bronze then (o.Acertos - m_alerta)/(m_bronze - m_alerta)*(p_bronze - p_alerta)+p_alerta
	      when o.Acertos < m_prata then (o.Acertos - m_bronze)/(m_prata - m_bronze)*(p_prata - p_bronze)+p_bronze
	      when o.Acertos < m_ouro then  (o.Acertos - m_prata)/(m_ouro - m_prata)*(p_ouro - p_prata)+p_prata
	      when o.Acertos < m_1 then  (o.Acertos - m_ouro)/(m_1 - m_ouro)*(p_1 - p_ouro)+p_ouro 
	      when o.Acertos < m_2 then  (o.Acertos - m_1)/(m_2 - m_1)*(p_2 - p_1)+p_1      
	      else p_2 end p_conhecimento
	,w.tipo
	,w.origem 
	from o
	left join banco.tabela w on o.Mes_ref = month(w.ref) and o.Ano_ref = year(w.ref)
	where lower(w.indicador) = 'conhecimento corporativo' AND lower(w.tipo) = 'coletivo' AND  UPPER(w.origem) = 'PRIMARIO'
)
,C6 AS (
with o as (
                select 
                ano_ref
                ,mes_ref
                ,avg(d.nota_dto) AS media_nota
                from banco.tabela d
                JOIN (SELECT * from banco.tabela  where upper(funcao) = 'E-MAIL' AND upper(area) = 'RELACIONAMENTO PRIMÁRIO') e ON d.matricula = e.matricula AND d.ano_ref = YEAR(e.mes_referencia) AND d.mes_ref = MONTH(e.mes_referencia)
                group by 
                ano_ref
                ,mes_ref 
         ) 
    select
    o.mes_ref
    ,o.ano_ref
    ,o.media_nota valor_dto_c
    ,case when o.media_nota < m_bronze then (o.media_nota - m_alerta)/(m_bronze - m_alerta)*(p_bronze - p_alerta)+p_alerta
          when o.media_nota < m_prata then (o.media_nota - m_bronze)/(m_prata - m_bronze)*(p_prata - p_bronze)+p_bronze
          when o.media_nota < m_ouro then  (o.media_nota - m_prata)/(m_ouro - m_prata)*(p_ouro - p_prata)+p_prata
          when o.media_nota < m_1 then  (o.media_nota - m_ouro)/(m_1 - m_ouro)*(p_1 - p_ouro)+p_ouro 
          when o.media_nota < m_2 then  (o.media_nota - m_1)/(m_2 - m_1)*(p_2 - p_1)+p_1      
          else p_2 end p_dto
    ,w.tipo
    ,w.origem 
    from o 
    left join banco.tabela w on cast(o.mes_ref as int) = month(w.ref) and cast(o.ano_ref as int) = year(w.ref)
    where lower(w.indicador) = 'dto' AND lower(w.tipo) = 'coletivo' AND  UPPER(w.origem) = 'PRIMARIO'
)
 , CG AS (
    SELECT * FROM banco.tabela
)
    
select *
from (
	select distinct
	t.DATA data_resolucao
	,t.matricula
	,t.Percent_Resolucao as percent_resolucao
	,t.Resolvidos as resolvidos
	,t.p_resolutividade
	,t.Recorrencia as recorrencia
	,t.resolvidos_rec
	,t.Producao as producao
	,t.p_produtividade
	,t.Assiduidade as assiduidade
	,C2.valor_ciclo_c
	,C2.p_ciclo p_ciclo_c
	,C5.valor_conhec_c
	,C5.p_conhecimento p_conhecimento_c
	,CAST(NULL AS double) pontos_ideia_c
	,C6.valor_dto_c
	,CAST(C6.p_dto AS double) p_dto_c
	,CG.reclamacoes
	,CG.p_reclamacoes 
	,CG.nps
	,CG.p_nps
	,CG.redes_sociais
	,CG.p_redes_sociais
	,t.ultima_atualizacao
	from tabela t
	LEFT JOIN C2 ON month(t.data) = C2.Mes_Ref AND YEAR(t.data) = C2.Ano_ref
	LEFT JOIN C5 ON month(t.data) = C5.Mes_Ref AND YEAR(t.data) = C5.Ano_ref
	LEFT JOIN C6 ON month(t.data) = C6.Mes_Ref AND YEAR(t.data) = C6.Ano_ref
	LEFT JOIN CG ON month(t.data) = CG.Mes_Ref AND YEAR(t.data) = CG.Ano_ref 
	UNION
	select *
	from (
	 SELECT
	 CASE WHEN CAST(ass.data_ref AS DATE) >= CAST('2022-07-01' AS DATE)
     THEN 
        CASE WHEN DAY(ass.data_ref) = 26 AND MONTH(ass.data_ref) = 12 THEN CAST(CONCAT(CAST(YEAR(ass.data_ref + INTERVAL '1' YEAR) AS varchar),'-',CAST(MONTH(ass.data_ref + INTERVAL '1' MONTH) AS varchar),'-','01') AS DATE)
             WHEN DAY(ass.data_ref) = 27 AND MONTH(ass.data_ref) = 12 THEN CAST(CONCAT(CAST(YEAR(ass.data_ref + INTERVAL '1' YEAR) AS varchar),'-',CAST(MONTH(ass.data_ref + INTERVAL '1' MONTH) AS varchar),'-','02') AS DATE)
             WHEN DAY(ass.data_ref) = 28 AND MONTH(ass.data_ref) = 12 THEN CAST(CONCAT(CAST(YEAR(ass.data_ref + INTERVAL '1' YEAR) AS varchar),'-',CAST(MONTH(ass.data_ref + INTERVAL '1' MONTH) AS varchar),'-','03') AS DATE)
             WHEN DAY(ass.data_ref) = 29 AND MONTH(ass.data_ref) = 12 THEN CAST(CONCAT(CAST(YEAR(ass.data_ref + INTERVAL '1' YEAR) AS varchar),'-',CAST(MONTH(ass.data_ref + INTERVAL '1' MONTH) AS varchar),'-','04') AS DATE)
             WHEN DAY(ass.data_ref) = 30 AND MONTH(ass.data_ref) = 12 THEN CAST(CONCAT(CAST(YEAR(ass.data_ref + INTERVAL '1' YEAR) AS varchar),'-',CAST(MONTH(ass.data_ref + INTERVAL '1' MONTH) AS varchar),'-','05') AS DATE)
             WHEN DAY(ass.data_ref) = 31 AND MONTH(ass.data_ref) = 12 THEN CAST(CONCAT(CAST(YEAR(ass.data_ref + INTERVAL '1' YEAR) AS varchar),'-',CAST(MONTH(ass.data_ref + INTERVAL '1' MONTH) AS varchar),'-','06') AS DATE)
        ELSE
            CASE WHEN DAY(ass.data_ref) = 26 THEN CAST(CONCAT(CAST(YEAR(ass.data_ref) AS varchar),'-',CAST(MONTH(ass.data_ref + INTERVAL '1' MONTH) AS varchar),'-','01') AS DATE)
                 WHEN DAY(ass.data_ref) = 27 THEN CAST(CONCAT(CAST(YEAR(ass.data_ref) AS varchar),'-',CAST(MONTH(ass.data_ref + INTERVAL '1' MONTH) AS varchar),'-','02') AS DATE)
                 WHEN DAY(ass.data_ref) = 28 THEN CAST(CONCAT(CAST(YEAR(ass.data_ref) AS varchar),'-',CAST(MONTH(ass.data_ref + INTERVAL '1' MONTH) AS varchar),'-','03') AS DATE)
                 WHEN DAY(ass.data_ref) = 29 THEN CAST(CONCAT(CAST(YEAR(ass.data_ref) AS varchar),'-',CAST(MONTH(ass.data_ref + INTERVAL '1' MONTH) AS varchar),'-','04') AS DATE)
                 WHEN DAY(ass.data_ref) = 30 THEN CAST(CONCAT(CAST(YEAR(ass.data_ref) AS varchar),'-',CAST(MONTH(ass.data_ref + INTERVAL '1' MONTH) AS varchar),'-','05') AS DATE)
                 WHEN DAY(ass.data_ref) = 31 THEN CAST(CONCAT(CAST(YEAR(ass.data_ref) AS varchar),'-',CAST(MONTH(ass.data_ref + INTERVAL '1' MONTH) AS varchar),'-','06') AS DATE)
            ELSE ass.data_ref  END 
        END     
		ELSE ass.data_ref END data_
	    ,ass.matricula         
	    ,NULL percent_resolucao  
	    ,NULL resolvidos   
	    ,NULL p_resolutividade                                     
	    ,NULL recorrencia
	    ,NULL resolvidos_rec
	    ,NULL producao    
	    ,CASE when falta_injustificada >=1 then 0
	          else  NULL end  as p_produtividade    
	    ,case when falta_injustificada >= 1 then 0 else 0 end as assiduidade                        
	    ,NULL valor_ciclo_c
	    ,NULL p_ciclo_c
	    ,NULL valor_conhec_c
	    ,NULL p_conhecimento_c
	    ,null pontos_ideia_c
	    ,NULL valor_dto_c
	    ,NULL p_dto_c
	    ,NULL reclamacoes
	    ,NULL p_reclamacoes
	    ,NULL nps
	    ,NULL p_nps
	    ,NULL redes_sociais
	    ,NULL p_redes_sociais
	    ,now() ultima_atualizacao
		from (SELECT data_ref, matricula, nome_colaborador, origem, falta_injustificada, faltas_justificadas, faltas_outros FROM banco.tabela) ass
		join( SELECT * from banco.tabela where upper(funcao) = 'E-MAIL' AND upper(area) = 'RELACIONAMENTO PRIMÁRIO') xx on ass.matricula = xx.matricula and month(xx.mes_referencia) = month (ass.data_ref) and year(xx.mes_referencia) = year (ass.data_ref)
	    left join c on ass.matricula = c.matricula and cast(ass.data_ref as date) = cast(c.data_ as date)
	    left join cr on ass.matricula = cr.matricula and cast(ass.data_ref as date) = cast(cr.data_ as date)
	    left join ac on  ass.matricula = ac.matricula and cast(ass.data_ref as date) = cast(ac.data_resolucao as date)
	    left join e on  ass.matricula = e.matricula and cast(ass.data_ref as date) = cast(e.data_resolucao as date)
		)
	)
	where  data_resolucao >= cast('2021-08-01' as date)
)

,recontato_mensal AS (
          SELECT 
          ano_ref
          ,mes_ref
          ,matricula
          ,resolvidos_rec
          ,recorrencia
          ,percent_recorrencia
          ,CASE WHEN Percent_recorrencia > m_alerta THEN 0 else case when Percent_recorrencia > m_bronze then
                 (Percent_recorrencia - m_alerta)/(m_bronze - m_alerta)*(p_bronze - p_alerta)+p_alerta
         when Percent_recorrencia > m_prata then 
                 (Percent_recorrencia - m_bronze)/(m_prata - m_bronze)*(p_prata - p_bronze)+p_bronze
         when Percent_recorrencia > m_ouro then 
                 (Percent_recorrencia - m_prata)/(m_ouro - m_prata)*(p_ouro - p_prata)+p_prata
         when Percent_recorrencia > m_1 then 
                 (Percent_recorrencia - m_ouro)/(m_1 - m_ouro)*(p_1 - p_ouro)+p_ouro 
         when Percent_recorrencia > m_2 then 
                 (Percent_recorrencia - m_1)/(m_2 - m_1)*(p_2 - p_1)+p_1      
         else p_2 end end p_recorrencia
          FROM(
			  SELECT
	          ano_ref
	          ,mes_ref
	          ,matricula
	          ,resolvidos_rec
	          ,recorrencia
	          , CASE WHEN case when resolvidos_rec is null then 1 else  recorrencia / resolvidos_rec end > 1 THEN 1 ELSE case when resolvidos_rec is null then 1 else  recorrencia /resolvidos_rec END end Percent_recorrencia
	          FROM (
			          SELECT
			          YEAR(data_resolucao) ano_ref
			          ,MONTH(data_resolucao) mes_ref
			          ,matricula
			          ,sum(resolvidos_rec) resolvidos_rec
			          ,sum(recorrencia) recorrencia
			          FROM cte_principal
			          GROUP BY 1,2,3
          		  )
            ) i
            left join banco.tabela w on i.mes_ref = month(w.ref) and i.ano_ref = year(w.ref)
    where lower(w.indicador) = 'recorrencia' and lower(rtrim(ltrim(w.tipo))) = 'individual' and UPPER(w.origem) = 'PRIMARIO'
)

,prejuizo_ind AS ( --Prejuizo por matrícula
           SELECT
    data_ref data_ref
    ,solicitante_matricula matricula
    ,sum(round(cast(reembolso_valor_prejuizo as double),0)) reembolso_valor_prejuizo
    ,sum(case when round(cast(reembolso_valor_prejuizo as double),0) <= 1000 then -5
          when round(cast(reembolso_valor_prejuizo as double),0) >1000 and round(cast(reembolso_valor_prejuizo as double),0) <=1800 then -10
          when round(cast(reembolso_valor_prejuizo as double),0) >1800 and round(cast(reembolso_valor_prejuizo as double),0) <=2600 then -15
          when round(cast(reembolso_valor_prejuizo as double),0) >2600 and round(cast(reembolso_valor_prejuizo as double),0) <=5000 then -20
          when round(cast(reembolso_valor_prejuizo as double),0) >5000 and round(cast(reembolso_valor_prejuizo as double),0) <=10000 then -30
          when round(cast(reembolso_valor_prejuizo as double),0) >10000 THEN -50 else null end) p_prejuizo
    from banco.tabela
    where CAST(data_ref AS date) >= CAST('2021-08-01' AS date)
    group by
    1,2
)

, dto_ind AS ( -- -- Esse indicador é calculado após agrupamento mensal.
    	WITH nota_dto AS (
    		SELECT 
    		ano_ref, mes_ref, matricula, dto
    		FROM (
	    		SELECT
			 	 YEAR(e.mes_referencia) ano_ref
			 	,MONTH(e.mes_referencia) mes_ref
		     	,e.matricula 
		     	,round(d.nota_dto, 2) AS dto
		     	FROM (SELECT * FROM banco.tabela WHERE lower(area) = 'relacionamento primário' AND lower(funcao) = 'e-mail') e
	    		LEFT JOIN banco.tabela d ON d.matricula = e.matricula AND d.ano_ref = YEAR(e.mes_referencia) AND d.mes_ref = MONTH(e.mes_referencia)
			)
		)		
		SELECT
		  r.ano_ref
		 ,r.mes_ref
	     ,r.matricula
	     ,round(COALESCE(r.dto, mt.m_ouro - 0.2), 2) AS dto
	     -- Pontuação no  - Quanto maior, melhor (<).
	     ,round(CASE 
       		WHEN round(COALESCE(r.dto, mt.m_ouro - 0.2), 2) < mt.m_bronze THEN (round(COALESCE(r.dto, mt.m_ouro - 0.2), 2) - mt.m_alerta) / (mt.m_bronze - mt.m_alerta) * (mt.p_bronze - mt.p_alerta) + mt.p_alerta
	      	WHEN round(COALESCE(r.dto, mt.m_ouro - 0.2), 2) < mt.m_prata THEN (round(COALESCE(r.dto, mt.m_ouro - 0.2), 2) - mt.m_bronze) / (mt.m_prata - mt.m_bronze) * (mt.p_prata - mt.p_bronze) + mt.p_bronze
	      	WHEN round(COALESCE(r.dto, mt.m_ouro - 0.2), 2) < mt.m_ouro THEN (round(COALESCE(r.dto, mt.m_ouro - 0.2), 2) - mt.m_prata) / (mt.m_ouro - mt.m_prata) * (mt.p_ouro - mt.p_prata) + mt.p_prata
	      	WHEN round(COALESCE(r.dto, mt.m_ouro - 0.2), 2) < mt.m_1 THEN (round(COALESCE(r.dto, mt.m_ouro - 0.2), 2) - mt.m_ouro) / (mt.m_1 - mt.m_ouro) * (mt.p_1 - mt.p_ouro) + mt.p_ouro 
	      	WHEN round(COALESCE(r.dto, mt.m_ouro - 0.2), 2) < mt.m_2 THEN (round(COALESCE(r.dto, mt.m_ouro - 0.2), 2) - mt.m_1) / (mt.m_2 - mt.m_1) * (mt.p_2 - mt.p_1) + mt.p_1      
	      	ELSE mt.p_2 END, 2) p_dto_i
	     FROM nota_dto r	     
	     LEFT JOIN banco.tabela mt ON r.mes_ref = MONTH(mt.ref) AND r.ano_ref = YEAR(mt.ref)
		 WHERE lower(mt.indicador) = 'dto' AND lower(rtrim(ltrim(mt.tipo))) = 'individual' AND lower(mt.origem) = 'primario' 
    )

, conhec_ind AS ( --Conhecimento Individual
		WITH nota_conhecimento AS (
    		SELECT 
    		ano_ref, mes_ref, matricula, conhecimento_corporativo
    		FROM (
	    		SELECT
			 	 YEAR(e.mes_referencia) ano_ref
			 	,MONTH(e.mes_referencia) mes_ref
		     	,e.matricula 
		     	,round(d.nota_conhecimento, 2) AS conhecimento_corporativo
		     	FROM (SELECT * FROM banco.tabela WHERE lower(area) = 'relacionamento primário' AND lower(funcao) = 'e-mail') e
	    		LEFT JOIN banco.tabela d ON d.matricula = e.matricula AND d.ano_ref = YEAR(e.mes_referencia) AND d.mes_ref = MONTH(e.mes_referencia)
			)
		)		
		SELECT
		  r.ano_ref
		 ,r.mes_ref
	     ,r.matricula 
	     ,round(COALESCE(r.conhecimento_corporativo, mt.m_ouro - 0.2), 2) AS conhecimento_corporativo
	     -- Pontuação no  - Quanto maior, melhor (<).
	     ,round(CASE 
       		WHEN round(COALESCE(r.conhecimento_corporativo, mt.m_ouro - 0.2), 2) < mt.m_bronze THEN (round(COALESCE(r.conhecimento_corporativo, mt.m_ouro - 0.2), 2) - mt.m_alerta) / (mt.m_bronze - mt.m_alerta) * (mt.p_bronze - mt.p_alerta) + mt.p_alerta
	      	WHEN round(COALESCE(r.conhecimento_corporativo, mt.m_ouro - 0.2), 2) < mt.m_prata THEN (round(COALESCE(r.conhecimento_corporativo, mt.m_ouro - 0.2), 2) - mt.m_bronze) / (mt.m_prata - mt.m_bronze) * (mt.p_prata - mt.p_bronze) + mt.p_bronze
	      	WHEN round(COALESCE(r.conhecimento_corporativo, mt.m_ouro - 0.2), 2) < mt.m_ouro THEN (round(COALESCE(r.conhecimento_corporativo, mt.m_ouro - 0.2), 2) - mt.m_prata) / (mt.m_ouro - mt.m_prata) * (mt.p_ouro - mt.p_prata) + mt.p_prata
	      	WHEN round(COALESCE(r.conhecimento_corporativo, mt.m_ouro - 0.2), 2) < mt.m_1 THEN (round(COALESCE(r.conhecimento_corporativo, mt.m_ouro - 0.2), 2) - mt.m_ouro) / (mt.m_1 - mt.m_ouro) * (mt.p_1 - mt.p_ouro) + mt.p_ouro 
	      	WHEN round(COALESCE(r.conhecimento_corporativo, mt.m_ouro - 0.2), 2) < mt.m_2 THEN (round(COALESCE(r.conhecimento_corporativo, mt.m_ouro - 0.2), 2) - mt.m_1) / (mt.m_2 - mt.m_1) * (mt.p_2 - mt.p_1) + mt.p_1      
	      	ELSE mt.p_2 END,2) p_cc_i
	     FROM nota_conhecimento r
	     LEFT JOIN banco.tabela mt ON r.mes_ref = MONTH(mt.ref) AND r.ano_ref = YEAR(mt.ref)
		 WHERE lower(mt.indicador) = 'conhecimento corporativo' AND lower(rtrim(ltrim(mt.tipo))) = 'individual' AND lower(mt.origem) = 'primario' 
)

, csat_ind as ( -- CSAT Individual
        WITH media_csat AS (
    		SELECT 
            YEAR(CASE WHEN DAY(a.data_das_respostas) >= 26 THEN a.data_das_respostas + INTERVAL '1' MONTH ELSE a.data_das_respostas END) ano_ref
            ,MONTH(CASE WHEN DAY(a.data_das_respostas) >= 26 THEN a.data_das_respostas + INTERVAL '1' MONTH ELSE a.data_das_respostas END) mes_ref
            ,a.matricula
            ,avg(a.media_) AS csat
            FROM banco.tabela a
            JOIN (SELECT * FROM banco.tabela WHERE upper(funcao) = 'E-MAIL' AND upper(area) = 'RELACIONAMENTO PRIMÁRIO') e 
            ON e.matricula = CAST(a.matricula AS integer) AND MONTH(a.data_das_respostas) = MONTH(e.mes_referencia) AND YEAR(a.data_das_respostas) = YEAR(e.mes_referencia)
            GROUP BY 1, 2, 3   	
    	) 

    	SELECT
        r.ano_ref
        ,r.mes_ref
        ,CAST(r.matricula AS integer) matricula
        ,round(r.csat, 2) AS media__csat
        -- Pontuação no  - Quanto maior, melhor (<=).
        ,round(CASE 
       		WHEN r.csat < mt.m_bronze THEN (r.csat - mt.m_alerta) / (mt.m_bronze - mt.m_alerta) * (mt.p_bronze - mt.p_alerta) + mt.p_alerta
	      	WHEN r.csat < mt.m_prata THEN (r.csat - mt.m_bronze) / (mt.m_prata - mt.m_bronze) * (mt.p_prata - mt.p_bronze) + mt.p_bronze
	      	WHEN r.csat < mt.m_ouro THEN (r.csat - mt.m_prata) / (mt.m_ouro - mt.m_prata) * (mt.p_ouro - mt.p_prata) + mt.p_prata
	      	WHEN r.csat < mt.m_1 THEN (r.csat - mt.m_ouro) / (mt.m_1 - mt.m_ouro) * (mt.p_1 - mt.p_ouro) + mt.p_ouro 
	      	WHEN r.csat < mt.m_2 THEN (r.csat - mt.m_1) / (mt.m_2 - mt.m_1) * (mt.p_2 - mt.p_1) + mt.p_1      
	      	ELSE mt.p_2 END, 2) p_csat     
       	FROM media_csat r
        LEFT JOIN banco.tabela mt ON r.mes_ref = MONTH(mt.ref) AND r.ano_ref = YEAR(mt.ref)
		WHERE lower(mt.indicador) = 'csat' AND lower(rtrim(ltrim(mt.tipo))) = 'individual' AND upper(mt.origem) = 'PRIMARIO'
        )

SELECT data_resolucao, zzz.matricula, percent_resolucao, resolvidos, p_resolutividade, yyy.percent_recorrencia, yyy.recorrencia, yyy.resolvidos_rec, yyy.p_recorrencia, media__csat, p_csat, producao, p_produtividade, assiduidade, xxx.p_prejuizo, aaa.dto, aaa.p_dto_i, bbb.conhecimento_corporativo, bbb.p_cc_i, valor_ciclo_c, p_ciclo_c
, valor_conhec_c, p_conhecimento_c, pontos_ideia_c, valor_dto_c, p_dto_c, reclamacoes, p_reclamacoes, nps, p_nps, redes_sociais, p_redes_sociais, jc.origem, jc.tipo, jc.matricula_supervisor, jc.supervisor, jc.matricula_coordenador, jc.coordenador, jc.matricula_gerente, jc.gerente
,zzz.ultima_atualizacao
FROM cte_principal zzz
LEFT JOIN recontato_mensal yyy ON YEAR(zzz.data_resolucao) = yyy.ano_ref AND MONTH(zzz.data_resolucao) = yyy.mes_ref AND zzz.matricula = yyy.matricula
LEFT JOIN prejuizo_ind xxx ON YEAR(zzz.data_resolucao) = YEAR(xxx.data_ref) AND MONTH(zzz.data_resolucao) = MONTH(xxx.data_ref) AND zzz.matricula = xxx.matricula
LEFT JOIN dto_ind aaa ON YEAR(zzz.data_resolucao) = aaa.ano_ref AND MONTH(zzz.data_resolucao) = aaa.mes_ref AND zzz.matricula = aaa.matricula
LEFT JOIN conhec_ind bbb ON YEAR(zzz.data_resolucao) = bbb.ano_ref AND MONTH(zzz.data_resolucao) = bbb.mes_ref AND zzz.matricula = bbb.matricula
LEFT JOIN csat_ind AS csat ON YEAR(zzz.data_resolucao) = csat.ano_ref AND MONTH(zzz.data_resolucao) = csat.mes_ref AND zzz.matricula = CAST(csat.matricula AS integer)
join (SELECT * from banco.tabela where funcao = 'E-MAIL' AND upper(area) = 'RELACIONAMENTO PRIMÁRIO' ) jc on jc.matricula  = zzz.matricula AND month(jc.mes_referencia) = month(zzz.data_resolucao) AND  year(jc.mes_referencia) = year(zzz.data_resolucao)