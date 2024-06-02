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