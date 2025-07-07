-- VISUALIZAÇÃO DA tb_locacao
select * from tb_locacao;

-- CRIAÇÃO DAS TABELAS

-- Cria tabela para os tipos de combustíveis
create table tb_combustiveis(
	idCombustivel integer primary key unique,
	tipoCombustivel text not null 
);

-- Cria a tabela para os dados dos clientes
create table tb_clientes(
	idCliente integer primary key unique,
	nomeCliente text not null,
	cidadeCliente text,
	estadoCliente text,
	paisCliente text
);

-- Cria a tabela para os dados dos vendedores
create table tb_vendedores(
	idVendedor integer primary key unique,
	nomeVendedor text not null,
	sexoVendedor integer,
	estadoVendedor text
);

-- Cria tabela para os dados dos carros
create table tb_carros (
    idCarro integer primary key unique,
    chassiCarro text not null unique,
    marcaCarro text not null,
    modeloCarro text not null,
    anoCarro integer,
    idCombustivel integer not null,

    foreign key (idCombustivel) references tb_combustiveis(idCombustivel)
);

-- Cria tabela cpara os dados das locações
create table tb_locacoes (
    idLocacao integer primary key unique,
    dataLocacao text not null,
    horaLocacao text not null,
    dataEntrega text not null,
    horaEntrega text not null,
    kmCarro integer not null,
    qtdDiaria integer not null,
    vlrDiaria NUMERIC not null,

    idCliente integer not null,
    idCarro integer not null,
    idVendedor integer not null,

    foreign key (idCliente) references tb_clientes(idCliente),
    foreign key (idCarro) references tb_carros(idCarro),
    foreign key (idVendedor) references tb_vendedores(idVendedor)
);

--  VISUALIZAÇÃO DAS TABELAS CRIADAS - ANTES DE INSERIR DADOS 
select * from tb_combustiveis;
select * from tb_clientes;
select * from tb_vendedores;
select * from tb_carros;
select * from tb_locacoes;


-- INSERÇÃO DE DADOS DA tb_locacao PARA AS TABELAS CRIADAS

-- Dados para a tabelas de combustíveis
insert into tb_combustiveis (tipoCombustivel)
select distinct tipoCombustivel
from tb_locacao
where tipoCombustivel is not null and tipoCombustivel != '';

-- Dados para a tabela de clientes
insert into tb_clientes (
	idCliente,
	nomeCliente,
	cidadeCliente,
	estadoCliente,
	paisCliente)
select
		idCliente,
		nomeCliente,
		cidadeCliente,
		estadoCliente,
		paisCliente
from tb_locacao
group by idCliente

-- Dados para a tabela de vendedores
insert into tb_vendedores (
	idVendedor,
	nomeVendedor,
	sexoVendedor,
	estadoVendedor)
select
	idVendedor,
	nomeVendedor,
	sexoVendedor,
	estadoVendedor
from tb_locacao
group by idVendedor;

-- Dados para a tabela de carros
insert into tb_carros (
    idCarro,
    chassiCarro,
    marcaCarro,
    modeloCarro,
    anoCarro,
    idCombustivel
)
select
    l.idCarro,
    l.chassiCarro,
    l.marcaCarro,
    l.modeloCarro,
    l.anoCarro,
    c.idCombustivel
from tb_locacao l
left join tb_combustiveis c
	on l.tipoCombustivel = c.tipoCombustivel
group by l.idCarro;

-- Dados para a tabela de locações
insert into tb_locacoes (
	idLocacao,
	dataLocacao,
	horaLocacao,
	dataEntrega,
	horaEntrega,
	kmCarro,
	qtdDiaria,
	vlrDiaria,
	idCliente,
	idCarro,
	idVendedor)
select
    idLocacao,
    dataLocacao,
    horaLocacao,
    dataEntrega,
    horaEntrega,
    kmCarro,
    qtdDiaria,
    vlrDiaria,
    idCliente,
    idCarro,
    idVendedor
from tb_locacao;

-- VISUALIZAÇÃO DAS TABELAS - APÓS INSERÇÃO DE DADOS
select * from tb_combustiveis;
select * from tb_clientes;
select * from tb_vendedores;
select * from tb_carros;
select * from tb_locacoes;

-- APAGAMOS A TABELA DE LOCAÇÃO
drop table  tb_locacao;

-- LIMPEZA DE DADOS

update tb_locacoes
set
   dataEntrega =  substr(
   					replace(dataEntrega, '.', ''), 1, 4) || '-' ||
                  substr(
                  	replace(dataEntrega, '.', ''), 5, 2) || '-' ||
                  substr(
                  	replace(dataEntrega, '.', ''), 7, 2),
	dataLocacao =
					substr(
   						replace(dataLocacao, '.', ''), 1, 4) || '-' ||
                  	substr(
                  		replace(dataLocacao, '.', ''), 5, 2) || '-' ||
                  	substr(
                  		replace(dataLocacao, '.', ''), 7, 2);

-- VISUALIZAÇÃO DAS TABELAS 
select * from tb_combustiveis;
select * from tb_clientes;
select * from tb_vendedores;
select * from tb_carros;
select * from tb_locacoes;

-- COMANDOS PARA APAGAR TABELAS
-- drop table tb_clientes;
-- drop table tb_vendedores;
-- drop table tb_carros;
-- drop table tb_combustiveis;
-- drop table tb_locacoes;
