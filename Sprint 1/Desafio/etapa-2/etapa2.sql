-- CRIANDO VIEWS

-- Cria view para os clientes
create view dim_clientes as
select
	idCliente as idCliente,
	nomeCliente,
	cidadeCliente,
	estadoCliente,
	paisCliente
from tb_clientes;

-- Cria view para os vendedores
create view dim_vendedores as
select
	idVendedor as idVendedor,
	nomeVendedor,
	case
		when sexoVendedor = 0 then 'Masculino'
		when sexoVendedor = 1 then 'Feminino'
		else 'Não Informado'
	end as sexo,
	estadoVendedor
from tb_vendedores;

--Cria view para os carros
create view dim_carros as
select
	c.idCarro as idCarro,
	c.chassiCarro,
	c.marcaCarro,
	c.modeloCarro,
	c.anoCarro,
	cb.tipoCombustivel
from tb_carros c
left join tb_combustiveis cb
	on c.idCombustivel = cb.idCombustivel;

-- Cria view para formatar o tempo de locação
create view dim_tempo as
select
	distinct
	dataHora,
	cast(strftime('%Y', dataHora) as integer) as ano,
	cast(strftime('%m', dataHora) as integer) as mes,
	cast(strftime('%d', dataHora) as integer) as dia,
	cast(strftime('%H', dataHora) as integer) as hora,
	cast(strftime('%W', dataHora) as integer) as semana_do_ano,
	case
		when strftime('%m', dataHora) in ('01', '02', '03') then '1º Trimestre'
        when strftime('%m', dataHora) in ('04', '05', '06') then '2º Trimestre'
        when strftime('%m', dataHora) in ('07', '08', '09') then '3º Trimestre'
        else '4º Trimestre'
	end as trimestre
from (
	select dataLocacao as dataHora from tb_locacoes
	union
	select dataEntrega as dataHora from tb_locacoes
);

-- Cria view fato
create view fato_locacoes as
select
    l.idLocacao,
    l.idCliente,
    l.idCarro,
    l.idVendedor,
    l.dataLocacao,
    l.dataEntrega,
    l.qtdDiaria,
    l.vlrDiaria,
    (l.qtdDiaria * l.vlrDiaria) as faturamento_total,
    strftime('%Y-%m-%d %H:00:00', l.dataLocacao) as tempoLocacao,
    strftime('%Y-%m-%d %H:00:00', l.dataEntrega) as tempoEntrega
from tb_locacoes l;

-- VISUALIZAÇÃO DAS DIMENSÕES E FATO 
select * from dim_tempo;
select * from dim_carros;
select * from dim_clientes;
select * from dim_vendedores;
select * from fato_locacoes;

-- COMANDOS PARA APAGAR AS VIEWS
-- drop view dim_tempo;
-- drop view dim_carros;
-- drop view dim_clientes;	
-- drop view dim_vendedores;
-- drop view fato_locacoes;




