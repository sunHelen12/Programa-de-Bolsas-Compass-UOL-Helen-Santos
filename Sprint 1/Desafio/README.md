# Etapas

1. ... [Etapa I - Modelo Relacional](./etapa-1/etapa1.sql)

## Criação das Tabelas

Para a normalização da `tb_locacao`, foi necessário desmembrar a tabela única em várias tabelas especializadas, cada uma com uma responsabilidade clara, a fim de eliminar a redundância de dados e garantir a integridade do modelo. O processo de criação foi o seguinte:

Primeiramente, foi criada a **`tb_combustiveis`**, uma tabela de domínio para padronizar os tipos de combustível.

- `idCombustivel`: É a chave primária (`PRIMARY KEY`) e `UNIQUE`, servindo como um identificador numérico único para cada tipo de combustível, como 'Gasolina' ou 'Etanol'.
    
- `tipoCombustivel`: Armazena o nome do combustível. A restrição `TEXT NOT NULL` garante que todo combustível tenha um nome e que não existam dois tipos com o mesmo nome.
    

Em seguida, a **`tb_clientes`** foi criada para armazenar exclusivamente as informações dos clientes, evitando a repetição de dados a cada locação.

- `idCliente`: É a chave primária e `UNIQUE`, garantindo que cada cliente seja único no sistema.
    
- `nomeCliente`: Guarda o nome do cliente, sendo `TEXT NOT NULL` por ser uma informação essencial.
    
- `cidadeCliente`, `estadoCliente`, `paisCliente`: Armazenam os dados de endereço, sendo opcionais pois podem não estar disponíveis.
    

De forma similar, a **`tb_vendedores`** centraliza as informações dos vendedores.

- `idVendedor`: Chave primária e `UNIQUE` que assegura um identificador exclusivo para cada vendedor.
    
- `nomeVendedor`: O nome do vendedor, campo obrigatório (`text not null`).
    
- `sexoVendedor`: Um código numérico para o sexo, definido como `INTEGER`.
    
- `estadoVendedor`: O estado de atuação do vendedor, um campo de texto opcional.
    

A tabela **`tb_carros`** foi desenhada para catalogar os veículos.

- `idCarro`: A chave primária e `UNIQUE` para identificar cada carro.
    
- `chassiCarro`: Um identificador textual único (`TEXT NOT NULL UNIQUE`) e obrigatório para cada veículo.
    
- `marcaCarro` e `modeloCarro`: Campos de texto obrigatórios para descrever o veículo.
    
- `anoCarro`: O ano de fabricação, armazenado como um número inteiro (`INTEGER`).
    
- `idCombustivel`: É a chave estrangeira (`FOREIGN KEY`) que cria uma conexão direta com a `tb_combustiveis`, garantindo que um carro só possa ser cadastrado com um tipo de combustível que realmente existe.
    

Por fim, a **`tb_locacoes`** foi criada como a tabela central, que une todas as informações.

- `idLocacao`: A chave primária e `UNIQUE` para cada transação de locação.
    
- `dataLocacao`, `horaLocacao`, `dataEntrega`, `horaEntrega`: Campos de texto obrigatórios (`NOT NULL`) que registram o período da locação.
    
- `kmCarro`, `qtdDiaria`, `vlrDiaria`: Métricas numéricas essenciais da locação, todas obrigatórias.
    
- `idCliente`, `idCarro`, `idVendedor`: São as chaves estrangeiras, todas `NOT NULL`, que garantem a integridade referencial do modelo. Elas asseguram que uma locação só possa se referir a um cliente, carro e vendedor que de fato existam em suas respectivas tabelas.

*Obs: Optou-se por não utilizar `AUTOINCREMENT` nas IDs, pra manter os valores originais da tabela antiga.
    
```
    create table tb_combustiveis(
        idCombustivel integer primary key unique,
        tipoCombustivel text not null
    );
```

Obtive esse retorno `select * from tb_combustiveis`:

![Tabela Combustíveis](/Sprint%201/Evidencias/Desafio/etapa-1/01-criando-tabela.png)

```
    create table tb_clientes(
        idCliente integer primary key unique,
        nomeCliente text not null,
        cidadeCliente text,
        estadoCliente text,
        paisCliente text
    );
```

Obtive esse retorno `select * from tb_clientes`:

![Tabela Clientes](/Sprint%201/Evidencias/Desafio/etapa-1/02-criando-tabela.png)

```
    create table tb_vendedores(
        idVendedor integer primary key unique,
        nomeVendedor text not null,
        sexoVendedor integer,
        estadoVendedor text
    );

```

Obtive esse retorno ao usar o `select * from tb_vendedores`:

![Tabela Vendedores](/Sprint%201/Evidencias/Desafio/etapa-1/03-criando-tabela.png)

```
    create table tb_carros (
        idCarro integer primary key unique,
        chassiCarro text not null unique,
        marcaCarro text not null,
        modeloCarro text not null,
        anoCarro integer,
        idCombustivel integer not null,

        foreign key (idCombustivel) references tb_combustiveis(idCombustivel)
    );
```

Obtive esse retorno ao usar o `select * from tb_carros`:

![Tabela Carros](/Sprint%201/Evidencias/Desafio/etapa-1/04-criando-tabela.png)      
    
```
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
```

Obtive esse retorno ao usar o `select * from tb_locacoes`:

![Tabela Locações](/Sprint%201/Evidencias/Desafio/etapa-1/05-criando-tabela.png)
    
## Inserção de Dados

Após a criação da estrutura, o próximo passo foi popular as novas tabelas com os dados da tabela original `tb_locacao`.

Para a **`tb_combustiveis`**, o comando `INSERT` foi utilizado em conjunto com o `SELECT DISTINCT` para extrair apenas os nomes únicos de combustíveis, evitando duplicatas e criando um catálogo limpo. A cláusula `WHERE` neste comando funciona como um filtro de qualidade de dados para garantir que apenas informações válidas sejam inseridas na tabela `tb_combustiveis`. Ele executa uma dupla verificação ao exigir que o valor na coluna `tipoCombustivel` atenda a duas condições ao mesmo tempo: primeiro, que ele **`IS NOT NULL`**, o que elimina quaisquer registros onde o campo de combustível esteja totalmente vazio ou nulo, e segundo, que ele seja diferente de uma string vazia (**`!= ''`**), que é um texto sem nenhum caractere. Ao combinar essas duas regras com o operador `AND`, o comando assegura que apenas registros com um nome de combustível real e preenchido sejam selecionados da tabela de origem, prevenindo a inserção de dados "sujos" ou incompletos no novo modelo organizado. 

Para as tabelas **`tb_clientes`** e **`tb_vendedores`**, a abordagem foi similar, utilizando `GROUP BY` pelo ID correspondente. Isso garante que, mesmo que um cliente ou vendedor apareça várias vezes na tabela original, ele seja inserido apenas uma vez na nova tabela, eliminando a redundância.

A inserção na **`tb_carros`** foi a mais elaborada. Foi utilizado o `LEFT JOIN` para conectar a tabela de locação com a `tb_combustiveis`. Essa conexão permitiu "traduzir" o nome do combustível (ex: "Gasolina") para o seu `idCombustivel` correspondente, que é o valor que era necessário armazenar. O `GROUP BY` foi usado novamente para garantir que cada carro fosse inserido apenas uma vez.

Por último, para a **`tb_locacoes`**, a inserção foi uma cópia direta dos dados transacionais da `tb_locacao` original. Como cada linha já representava uma locação única, não foi necessário agrupar ou filtrar, apenas transferir os dados para a nova estrutura, agora devidamente conectada às outras tabelas através dos IDs.

```
    insert into tb_combustiveis (tipoCombustivel)
    select distinct tipoCombustivel
    from tb_locacao
    where tipoCombustivel is not null and tipoCombustivel != '';
```

Obtive esse retorno ao usar o `select * from tb_combustiveis`:

![Tabela Combustíveis](/Sprint%201/Evidencias/Desafio/etapa-1/06-inserindo-dados.png)

```
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
```

Obtive esse retorno ao usar o `select * from tb_clientes`:

![Tabela Clientes](/Sprint%201/Evidencias/Desafio/etapa-1/07-inserindo-dados.png)

```
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
```

Obtive esse retorno ao usar o `select * from tb_vendedores`:

![Tabela Vendedores](/Sprint%201/Evidencias/Desafio/etapa-1/08-inserindo-dados.png)

```    
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
```

Obtive esse retorno ao usar o `select * from tb_carros`:

![Tabela Carros](/Sprint%201/Evidencias/Desafio/etapa-1/09-inserindo-dados.png)
    
```    
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
```

Obtive esse retorno ao usar o `select * from tb_locacoes`:

![Tabela Locações](/Sprint%201/Evidencias/Desafio/etapa-1/10-inserindo-dados.png)
    
## Apagando Tabela `tb_locacao`

Foi apagada a tabela tb_locacao, pois ela não seria mais necessária após a inserção de seus dados nas tabelas criadas.

O comando utilizado para isso foi:

```
drop table  tb_locacao;
```

(Depois)Obtive esse retorno ao usar o `select * from tb_locacao`:

![Tabela Locação](/Sprint%201/Evidencias/Desafio/etapa-1/11-apagando-tabela.png)

## Limpando Dados

Na `tb_locacoes`, após a inserção dos dados, foi identificada a necessidade de uma limpeza nos dados. O problema estava nas colunas de data (`dataEntrega` e `dataLocacao`), que foram importadas um formato incorreto contendo pontos (ex: '20.150.112'). Para corrigir isso, foi utilizado um comando `UPDATE`. A função `REPLACE()` foi usada para remover os pontos, e a função `SUBSTR()` foi aplicada para recortar os trechos de ano, mês e dia, que foram então concatenados com hifens para formar o `YYYY-MM-DD`, que é o formato de data padrão.

```

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

```

**(Antes)** Obtive esse retorno ao usar o `select * from tb_locacoes`:

![Tabela Locações(Antes)](/Sprint%201/Evidencias/Desafio/etapa-1/10-inserindo-dados.png)

**(Depois)** Obtive esse retorno ao usar o `select * from tb_locacoes`:

![Tabela Locações(Depois)](/Sprint%201/Evidencias/Desafio/etapa-1/12-limpeza-de-dados-tabela.png)

## Diagrama

Por fim, foi gerado o diagrama representando o modelo relacional bem estruturado, onde a tabela `tb_locacoes` funciona como o núcleo central, registrando cada evento de negócio. Ela se conecta diretamente às tabelas `tb_clientes`, `tb_vendedores` e `tb_carros` através de relações de **um-para-muitos**, o que significa que um único cliente pode realizar múltiplas locações, um vendedor pode efetuar várias vendas, e um mesmo carro pode ser alugado diversas vezes ao longo do tempo. Adicionalmente, a tabela `tb_carros` possui sua própria relação de um-para-muitos com a `tb_combustiveis`, indicando que cada carro utiliza um tipo específico de combustível, mas um mesmo tipo de combustível pode ser usado por vários carros diferentes no catálogo. Essa estrutura garante a integridade dos dados e elimina a redundância, pois as informações de clientes, vendedores e carros são armazenadas uma única vez em suas respectivas tabelas.

Obtive esse retorno ao gerar o diagrama:

![Diagrama (Modelo Relaciobnal)](/Sprint%201/Evidencias/Desafio/etapa-1/13-diagrama.png)

2. ... [Etapa II - Modelo Dimensional](./etapa-2/etapa2.sql)

## Criação das VIEWs

A criação das `VIEW`s representa a transição de um modelo de dados puramente operacional para um modelo dimensional, otimizado para análise e inteligência de negócio. A escolha de dividir a estrutura em dimensões e fatos segue a metodologia do Esquema Estrela (*Star Schema*) , onde cada `VIEW` desempenha um papel estratégico para simplificar e enriquecer as consultas.

A **`dim_clientes`** e a **`dim_vendedores`** foram criadas como uma camada de abstração sobre as tabelas base. Elas fornecem uma visão limpa e descritiva das entidades "quem", permitindo que os relatórios exibam nomes e atributos em vez de apenas IDs. Na `dim_vendedores`, a utilização da expressão `CASE` para traduzir os códigos numéricos de sexo (`0` e `1`) para textos legíveis ('Masculino', 'Feminino') é um exemplo de enriquecimento de dados, garantindo que a camada de análise já entregue a informação tratada e pronta para o consumo humano, sem a necessidade de lógicas adicionais em cada consulta.

De forma similar, a **`dim_carros`** foi projetada para consolidar todas as informações relevantes sobre o "o quê" da análise. Ao realizar o `LEFT JOIN` com a `tb_combustiveis` dentro da própria `VIEW`, o modelo se torna mais eficiente. Isso evita que o analista precise realizar essa mesma junção repetidamente, já entregando uma dimensão completa que inclui não apenas os dados do carro, mas também a descrição do seu tipo de combustível.

A criação da **`dim_tempo`** é um dos pilares da modelagem dimensional. Ela transforma a data, transforma a data, um campo transacional aparentemente simples, em uma poderosa dimensão de análise. Ao extrair atributos como ano, mês, dia e trimestre, ela permite que os dados de negócio sejam agrupados e analisados sob qualquer perspectiva temporal, respondendo a perguntas como "qual o faturamento por trimestre?" de forma direta e eficiente.

Por fim, a **`fato_locacoes`** foi estabelecida como o núcleo do modelo. Ela centraliza as chaves que se conectam a todas as dimensões e, mais importante, contém as métricas de negócio. A decisão de incluir uma coluna calculada, o `faturamento_total`, dentro da `VIEW` é estratégica: ela padroniza a regra de negócio, garantindo que todos os relatórios e análises utilizem o mesmo cálculo, o que confere consistência e confiabilidade aos resultados e simplifica drasticamente a complexidade das consultas finais.

```
  create view dim_clientes as
  select
    idCliente as idCliente,
    nomeCliente,
    cidadeCliente,
    estadoCliente,
    paisCliente
  from tb_clientes;
```

Obtive esse retorno `select * from dim_clientes`:

![View Clientes](/Sprint%201/Evidencias/Desafio/etapa-2/01-criando-view.png)

```
 create view dim_vendedores as
    select
        idVendedor as idVendedor,
        nomeVendedor,
        case
            when sexoVendedor = 0 then 'Masculino'
            when sexoVendedor = 1 then 'Feminino'
            else 'Não Informado'
        end as sexo,
        estadoVendedor
    from tb_vendedores;
```

Obtive esse retorno `select * from dim_vendedores`:

![View Vendedores](/Sprint%201/Evidencias/Desafio/etapa-2/02-criando-view.png)

```
create view dim_carros as
select
    c.idCarro as idCarro,
    c.chassiCarro,
    c.marcaCarro,
    c.modeloCarro,
    c.anoCarro,
    cb.tipoCombustivel
from tb_carros c
left join tb_combustiveis cb
    on c.idCombustivel = cb.idCombustivel;
```

Obtive esse retorno `select * from dim_carros`:

![View Carros](/Sprint%201/Evidencias/Desafio/etapa-2/03-criando-view.png)

```
create view dim_tempo as
select
	distinct
	dataLocacao,
	cast(strftime('%Y', dataLocacao) as integer) as ano,
	cast(strftime('%m', dataLocacao) as integer) as mes,
	cast(strftime('%d', dataLocacao) as integer) as dia,
	cast(strftime('%W', dataLocacao) as integer) as semana_do_ano,
	case
		when strftime('%m', dataLocacao) in ('01', '02', '03') then '1º Trimestre'
        when strftime('%m', dataLocacao) in ('04', '05', '06') then '2º Trimestre'
        when strftime('%m', dataLocacao) in ('07', '08', '09') then '3º Trimestre'
        else '4º Trimestre'
	end as trimestre
from tb_locacoes;

```

Obtive esse retorno `select * from dim_tempo`:

![View Tempo](/Sprint%201/Evidencias/Desafio/etapa-2/04-criando-view.png)

```
create view fato_locacoes as
select
    idLocacao,
    idCliente,
    idCarro,
    idVendedor,
    dataLocacao,
    qtdDiaria,
    vlrDiaria,
    (qtdDiaria * vlrDiaria) as faturamento_total
from tb_locacoes;
```

Obtive esse retorno `select * from fato_locacoes`:

![View da Fatos](/Sprint%201/Evidencias/Desafio/etapa-2/05-criando-view.png)

## Diagrama

O diagrama gerado permite visualizar perfeitamente a estrutura do **Esquema Estrela** que foi construído com as `VIEW`s. No centro da estrela, temos a **`fato_locacoes`**, que contém os dados quantitativos e as métricas do negócio, como `vlrDiaria` e o `faturamento_total`. Orbitando ao redor dela, como pontas da estrela, estão as **`dimensoes`**: `dim_clientes`, `dim_vendedores`, `dim_carros` e `dim_tempo`. Cada dimensão fornece o contexto descritivo para os números, respondendo às perguntas de "quem?", "o quê?" e "quando?". As linhas que conectam as dimensões à tabela de fatos representam as **ligações lógicas** que são realizadas através de comandos `JOIN` durante uma consulta, utilizando as chaves correspondentes (como `idCliente`, `idCarro`, etc.) para cruzar as informações e permitir análises detalhadas, como o faturamento por marca de carro ou por trimestre.


Obtive esse retorno ao gerar o diagrama:

![View do Diagrama (Modelo Dimensional)](/Sprint%201/Evidencias/Desafio/etapa-2/06-diagrama.png)