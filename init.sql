CREATE SCHEMA "pcp" AUTHORIZATION postgres;
CREATE TABLE "pcp"."Plano" (
    "codigo" int PRIMARY KEY,
    "descricao do Plano" VARCHAR(50),
    "inicioVenda" VARCHAR(50),
    "FimVenda" VARCHAR(50),
    "inicoFat" VARCHAR(50),
    "finalFat" VARCHAR(50),
    "usuarioGerador" VARCHAR(50),
    "dataGeracao" VARCHAR(50)
);

--pcp.usuarios definition
--Drop table
--DROP TABLE pcp.usuarios;
CREATE TABLE pcp.usuarios (
	codigo varchar NOT NULL,
	nome varchar NOT NULL,
	senha varchar NOT NULL,
	CONSTRAINT usuarios_pkey PRIMARY KEY (codigo)
);


-- pcp."LoteporPlano" definition
-- Drop table
-- DROP TABLE pcp."LoteporPlano";

CREATE TABLE pcp."LoteporPlano" (
	plano varchar NOT NULL,
	lote varchar NOT NULL,
	nomelote varchar NULL,
	id serial4 NOT NULL,
	CONSTRAINT "LoteporPlano_pkey" PRIMARY KEY (id)
);


-- pcp."colecoesPlano" definition
-- Drop table
-- DROP TABLE pcp."colecoesPlano";

CREATE TABLE pcp."colecoesPlano" (
	plano varchar NOT NULL,
	colecao varchar NOT NULL,
	id serial4 NOT NULL,
	nomecolecao varchar NULL,
	CONSTRAINT "colecoesPlano_pkey" PRIMARY KEY (id)
);


-- pcp."tipoNotaporPlano" definition
-- Drop table
-- DROP TABLE pcp."tipoNotaporPlano";
CREATE TABLE pcp."tipoNotaporPlano" (
	"tipo nota" varchar NOT NULL,
	nome varchar NOT NULL,
	plano varchar NOT NULL,
	id serial4 NOT NULL,
	CONSTRAINT "tipoNotaporPlano_pkey" PRIMARY KEY (id)
);