/*
 * EX12
 * Apresente a query para listar código, nome e data de nascimento dos dependentes do vendedor
 * com menor valor total bruto em vendas (não sendo zero). As colunas presentes no resultado
 * devem ser cddep, nmdep, dtnasc e valor_total_vendas.
 *
 * Observação: Apenas vendas com status concluído.
 */

with  vendedor as (
	select
		cdvdd,
		sum(qtd * vrunt) as venda_total
	from tbvendas
	where  status = 'Concluído'
	group by cdvdd
	having sum(qtd * vrunt) > 0
	order by venda_total
	limit 1
)
select
	dp.cddep,
	dp.nmdep,
	dp.dtnasc,
	v.venda_total
from tbdependente dp
join vendedor v
	on dp.cdvdd = v.cdvdd;

