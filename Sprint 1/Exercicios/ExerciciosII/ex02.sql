/*
 * SEÇÃO 6: LINGUAGEM SQL - EXERCÍCIOS II
 * EXPORTAÇÃO DE DADOS

 * Exportar o resultado da query que obtém as 5 editoras com maior quantidade de livros 
 * na biblioteca para um arquivo CSV. Utilizar o caractere | (pipe) como separador. 
 * Lembre-se que o conteúdo do seu arquivo deverá respeitar a sequência de colunas e 
 * seus respectivos nomes de cabeçalho que listamos abaixo:
 * 
 * CodEditora NometEditora QuantidadeLivros
 * 
 */

select
	e.codeditora as CodEditora,
	e.nome as NomeEditora,
	count(l.cod) as QuantidadeLivros	
from livro l
join editora e on l.editora = e.codeditora
group by e.codeditora, e.nome 
order by QuantidadeLivros desc
limit 5;


