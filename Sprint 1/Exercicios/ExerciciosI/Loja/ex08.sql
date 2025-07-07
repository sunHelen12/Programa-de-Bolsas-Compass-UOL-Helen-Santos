/*
 * EX08
 * Apresente a query para listar o código e o nome do vendedor com maior número de vendas
 * (contagem), e que estas vendas estejam com o status concluída.  As colunas presentes
 * no resultado devem ser, portanto, cdvdd e nmvdd.
*/


select
	vdd.cdvdd,
	vdd.nmvdd
from tbvendas ven
join tbvendedor vdd
	on vdd.cdvdd  = ven.cdvdd
where ven.status = 'Concluído'
group by vdd.cdvdd, vdd.nmvdd
order by count(ven.cdven) desc
limit 1;
