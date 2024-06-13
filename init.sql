-- Criar schema se não existir
DO
$$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_namespace WHERE nspname = 'pcp') THEN
        CREATE SCHEMA "pcp" AUTHORIZATION postgres;
    END IF;
END
$$;

-- Criar tabela Plano se não existir
CREATE TABLE IF NOT EXISTS "pcp"."Plano" (
    "codigo" int PRIMARY KEY,
    "descricao do Plano" VARCHAR(50),
    "inicioVenda" VARCHAR(50),
    "FimVenda" VARCHAR(50),
    "inicoFat" VARCHAR(50),
    "finalFat" VARCHAR(50),
    "usuarioGerador" VARCHAR(50),
    "dataGeracao" VARCHAR(50)
);

-- Criar tabela usuarios se não existir
CREATE TABLE IF NOT EXISTS pcp.usuarios (
    codigo varchar NOT NULL,
    nome varchar NOT NULL,
    senha varchar NOT NULL,
    CONSTRAINT usuarios_pkey PRIMARY KEY (codigo)
);

-- Criar tabela LoteporPlano se não existir
CREATE TABLE IF NOT EXISTS pcp."LoteporPlano" (
    plano varchar NOT NULL,
    lote varchar NOT NULL,
    nomelote varchar NULL,
    id serial4 NOT NULL,
    CONSTRAINT "LoteporPlano_pkey" PRIMARY KEY (id)
);

-- Criar tabela colecoesPlano se não existir
CREATE TABLE IF NOT EXISTS pcp."colecoesPlano" (
    plano varchar NOT NULL,
    colecao varchar NOT NULL,
    id serial4 NOT NULL,
    nomecolecao varchar NULL,
    CONSTRAINT "colecoesPlano_pkey" PRIMARY KEY (id)
);

-- Criar tabela tipoNotaporPlano se não existir
CREATE TABLE IF NOT EXISTS pcp."tipoNotaporPlano" (
    "tipo nota" varchar NOT NULL,
    nome varchar NOT NULL,
    plano varchar NOT NULL,
    id serial4 NOT NULL,
    CONSTRAINT "tipoNotaporPlano_pkey" PRIMARY KEY (id)
);
CREATE TABLE pcp.ordemprod (
	"codProduto" text NULL,
	numeroop text NULL,
	"codSortimento" text NULL,
	"seqTamanho" text NULL,
	total_pcs float8 NULL,
	"codTipoOP" int8 NULL,
	"seqAtual" text NULL,
	codreduzido text NULL,
	id text NULL,
	ocorrencia_sku float8 NULL,
	"qtdAcumulada" float8 NULL
);

CREATE TABLE pcp."SKU" (
	"codSKU" text NOT NULL,
	"codItemPai" text NULL,
	"codSortimento" int8 NULL,
	"codCor" text NULL,
	"codSeqTamanho" int8 NULL,
	"nomeSKU" text NULL,
	CONSTRAINT sku_pk PRIMARY KEY ("codSKU")
);