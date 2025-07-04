/*
 * EX06
 * Apresente a query para listar o autor com maior número
 * de livros publicados. O resultado deve conter apenas as
 * colunas codautor, nome, quantidade_publicacoes.
 */

select
    a.codautor,
    a.nome,
    count(l.cod) as quantidade_publicacoes
from autor a
join livro l on a.codautor = l.autor
group by a.codautor, a.nome
order by quantidade_publicacoes desc
limit 1;
