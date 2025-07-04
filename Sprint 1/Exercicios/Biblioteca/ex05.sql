/*
 * EX05
 * Apresente a query para listar o nome dos autores que publicaram livros através
 * de editoras NÃO situadas na região sul do Brasil. Ordene o resultado pela coluna nome,
 * em ordem crescente. Não podem haver nomes repetidos em seu retorno.
 *
 */

select distinct
    a.nome
from autor a
join livro l on a.codautor = l.autor
join editora e on l.editora = e.codeditora
join endereco en on e.endereco = en.codendereco
where en.estado not in ('RIO GRANDE DO SUL', 'PARANÁ', 'SANTA CATARINA')
order by a.nome;

