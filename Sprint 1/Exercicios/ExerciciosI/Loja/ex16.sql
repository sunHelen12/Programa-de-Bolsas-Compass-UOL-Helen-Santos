/* EX16
 * Apresente a query para listar a quantidade média vendida de cada produto agrupado por
 * estado da federação. As colunas presentes no resultado devem ser estado e nmprod
 * e quantidade_media. Considere arredondar o valor da coluna quantidade_media na
 * quarta casa decimal. Ordene os resultados pelo estado (1º) e nome do produto (2º).
 *
 * Obs: Somente vendas concluídas.
 */

select
	estado,
	nmpro,
	round(avg(qtd), 4) as quantidade_media
from tbvendas
where status = 'Concluído'
group by estado, nmpro
order by estado, nmpro;
