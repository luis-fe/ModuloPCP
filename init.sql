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

CREATE TABLE pcp.justificativa (
	ordemprod varchar NULL,
	fase varchar NULL,
	justificativa varchar NULL,
	ip varchar NULL
);

CREATE TABLE pcp.leadtime_categorias (
	codfase varchar NULL,
	categoria varchar NULL,
	leadtime varchar NULL,
	limite_atencao varchar NULL
);

CREATE TABLE pcp.controle_requisicao_csw (
	rotina varchar NULL,
	inicio varchar NULL,
	fim varchar NULL,
	ip_origem varchar NULL,
	"tempo_processamento(s)" float8 NULL,
	status varchar NULL,
	etapa1 varchar NULL,
	etapa1_tempo float8 NULL,
	etapa2 varchar NULL,
	etapa2_tempo float8 NULL,
	etapa3 varchar NULL,
	etapa3_tempo float8 NULL,
	etapa4 varchar NULL,
	etapa4_tempo float8 NULL,
	etapa5 varchar NULL,
	etapa5_tempo float8 NULL,
	etapa6 varchar NULL,
	etapa6_tempo float8 NULL,
	etapa7 varchar NULL,
	etapa7_tempo float8 NULL,
	etapa8 varchar NULL,
	etapa8_tempo float8 NULL,
	etapa9 varchar NULL,
	etapa9_tempo float8 NULL,
	etapa10 varchar NULL,
	etapa10_tempo float8 NULL,
	etapa11 varchar NULL,
	etapa11_tempo float8 NULL,
	etapa12 varchar NULL,
	etapa12_tempo float8 NULL,
	etapa13 varchar NULL,
	etapa13_tempo float8 NULL,
	etapa14 varchar NULL,
	etapa14_tempo float8 NULL,
	etapa15 varchar NULL,
	etapa15_tempo float8 NULL,
	etapa16 varchar NULL,
	etapa16_tempo float8 NULL,
	etapa17 varchar NULL,
	etapa17_tempo float8 NULL,
	etapa18 varchar NULL,
	etapa18_tempo float8 NULL,
	etapa19 varchar NULL,
	etapa19_tempo float8 NULL,
	etapa20 varchar NULL,
	etapa20_tempo float8 NULL,
	etapa21_tempo float8 NULL,
	etapa21 varchar NULL,
	etapa22_tempo float8 NULL,
	etapa22 varchar NULL,
	etapa23 varchar NULL,
	etapa23_tempo float8 NULL,
	etapa24 varchar NULL,
	etapa24_tempo float8 NULL,
	etapa25 varchar NULL,
	etapa25_tempo float8 NULL,
	"usoCpu1" varchar NULL,
	"usoCpu2" varchar NULL,
	"usoCpu3" varchar NULL,
	"usoCpu4" varchar NULL,
	"usoCpu6" varchar NULL,
	"usoCpu5" varchar NULL,
	"usoCpu11" varchar NULL,
	"usoCpu8" varchar NULL,
	"usoCpu7" varchar NULL,
	"usoCpu9" varchar NULL,
	"usoCpu10" varchar NULL
);

CREATE TABLE pcp."DashbordTV" (
	empresa varchar NULL,
	tiponota varchar NULL,
	exibi_todas_empresas varchar NULL,
	id varchar NOT NULL,
	CONSTRAINT configuracao_pk PRIMARY KEY (id)
);

CREATE OR REPLACE FUNCTION pcp.concatenatiponota()
 RETURNS trigger
 LANGUAGE plpgsql
AS $function$
BEGIN
    NEW."cod_desTipoNota" := NEW.codtiponota || '-' || NEW."descTipoNota";
    RETURN NEW;
END;
$function$
;

CREATE TABLE pcp."tipoNotaMonitor" (
	codtiponota varchar NULL,
	"descTipoNota" varchar NULL,
	"cod_desTipoNota" varchar NULL
);

create trigger trg_concatenate_notamon before
insert
    or
update
    on
    pcp."tipoNotaMonitor" for each row execute procedure pcp.concatenatiponota();