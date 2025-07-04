/*
 * EX04
 * Apresente a query para listar a quantidade de livros publicada por cada autor.
 * Ordenar as linhas pela coluna nome (autor), em ordem crescente. Além desta,
 * apresentar as colunas codautor, nascimento e quantidade (total de livros de sua autoria).
 *
 * Dica para ordenação: Utilize Replace
 */

with autores as(
	select
		a.nome,
		a.codautor,
		a.nascimento,
		count(*) as quantidade,
		replace(replace(replace(replace(replace(
        replace(replace(replace(replace(replace(
        upper(a.nome),
        'Á', 'A'), 'À', 'A'), 'Ã', 'A'),
        'É', 'E'), 'Ê', 'E'),
        'Í', 'I'), 'Ó', 'O'),
        'Ô', 'O'), 'Ú', 'U'),
        'Ç', 'C') as nomes_com_replace
   	from autor a
   	join livro l  on a.codautor = l.autor
	group by a.codautor, a.nascimento, a.nome
)
select
	nome,
	codautor,
	nascimento,
	quantidade
from autores
order by nomes_com_replace;

