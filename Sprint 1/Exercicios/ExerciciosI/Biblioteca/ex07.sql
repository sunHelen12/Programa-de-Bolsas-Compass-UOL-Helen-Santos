/*
 * EX07
 * Apresente a query para listar o nome dos autores com nenhuma publicação.
 * Apresentá-los em ordem crescente.
 */

select
  a.nome
from autor a
left join livro l on a.codautor = l.autor
where l.cod is null
order by a.nome;
