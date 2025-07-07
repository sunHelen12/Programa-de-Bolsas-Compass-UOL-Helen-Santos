/*
 * /E13
 * Apresente a query para listar os 10 produtos menos vendidos pelos canais de E-Commerce ou Matriz
 * (Considerar apenas vendas concluídas).  As colunas presentes no resultado devem ser cdpro, nmcanalvendas,
 * nmpro e quantidade_vendas.
 */

select
	cdpro,
	nmcanalvendas,
	nmpro,
	sum(qtd) as quantidade_vendas
from  tbvendas
where status = 'Concluído'
	and nmcanalvendas in ('Ecommerce', 'Matriz')
group by cdpro, nmpro, nmcanalvendas
order by quantidade_vendas
limit 10;
