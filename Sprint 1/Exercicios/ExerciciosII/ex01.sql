/*
 * SEÇÃO 6: LINGUAGEM SQL - EXERCÍCIOS II
 * EXPORTAÇÃO DE DADOS
 * 
 * Exportar o resultado da query que obtém os 10 livros mais caros para um arquivo CSV. Utilizar 
 * o caractere ";" (ponto e vírgula) como separador. Lembre-se que o conteúdo do seu arquivo deverá 
 * respeitar a sequência de colunas e seus respectivos nomes de cabeçalho que listamos abaixo:
 * 
 * CodLivro Titulo CodAutor NomeAutor Valor CodEditora NomeEditora
 * 
 */

select
   l.cod as CodLivro,
   l.titulo as Titulo,
   l.autor as CodAutor,
   a.nome as NomeAutor,
   l.valor as Valor,
   l.editora as CodEditora,
   e.nome as NomeEditora   
from livro l 
join editora e on  e.codeditora = l.editora
join autor a on a.codautor = l.autor
order by valor desc
limit 10;

