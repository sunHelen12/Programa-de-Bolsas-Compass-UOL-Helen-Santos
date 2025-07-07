/*
 * EX03
 * Apresente a query para listar as 5 editoras com mais livros na biblioteca.
 * O resultado deve conter apenas as colunas quantidade, nome, estado e cidade.
 * Ordenar as linhas pela coluna que representa a quantidade de livros em
 * ordem decrescente.
 */

select
	count(l.cod) as quantidade,
	e.nome,
	en.estado,
	en.cidade
from editora e
join livro l on e.codeditora = l.editora
join endereco en on e.endereco = en.codendereco
group by e.nome, e.codeditora, en.estado, en.cidade
order by quantidade desc
limit 5;
