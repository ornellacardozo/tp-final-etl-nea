# Exportaciones del NEA — pipeline ETL

Pipeline ETL en Python que descarga las exportaciones de las cuatro provincias
del NEA (Chaco, Corrientes, Formosa y Misiones) desde la API de datos abiertos
del Estado argentino y produce un dataset analítico de **1.408 filas × 13
columnas**: una fila por provincia, año y país de destino, entre 1993 y 2024.

TP Final · Unidad II — Fundamentos de la Programación
Diplomatura en Data Analytics e IA Aplicada — UNNE / Extender

---

## Qué hace

```
   API datos.gob.ar          data/raw/*.json         data/processed/
   (INDEC, 8 llamadas)  -->  (crudo, sin tocar) -->  exportaciones_nea.csv
                                                     resumen.json
        EXTRACT                  TRANSFORM              CHEQUEAR + LOAD
```

**Extract** (`src/extract.py`) hace dos llamadas por provincia —una de destinos
más el total provincial, otra de rubros— y guarda cada respuesta tal como llega
en `data/raw/`, junto con el orden de las columnas: sin ese dato no se sabría
qué número corresponde a qué destino. No limpia ni calcula nada.

**Transform** (`src/transform.py`) es donde pasa lo interesante:

1. Pasa los datos de formato **ancho** (una columna por serie, como los manda
   la API) a formato **largo**: una fila por observación.
2. Agrega columnas derivadas: región geoeconómica del destino, década y
   participación del destino en el total de la provincia.
3. Calcula la **variación interanual** de cada serie (provincia + destino)
   usando un diccionario como índice, para no recorrer toda la lista en cada
   fila.
4. Arma el **ranking de destinos** dentro de cada grupo (provincia, año) y
   marca los tres principales.
5. Hace un **join** por clave compuesta `(provincia, año)` contra los datos de
   rubros, para pegarle a cada fila el rubro más exportado ese año y el
   porcentaje que representaron los productos primarios. Es un *left join*: si
   no hay match, las columnas quedan nulas pero la fila no se pierde.

**Load** (`src/load.py`) valida antes de publicar. Cuatro chequeos críticos
—cantidad de filas, esquema de columnas, unicidad de la clave
`(provincia, año, destino)` y rango de valores— cortan el pipeline con una
excepción si fallan: mejor no entregar nada que entregar un CSV roto. Un quinto
chequeo, de cobertura de nulos, solo deja una advertencia. Recién después
escribe las tres salidas: el CSV (para personas), el resumen JSON (para
programas) y una línea en el log (para auditar).

El CSV y el JSON se escriben en modo `"w"`, así que correr el pipeline dos veces
deja exactamente el mismo resultado. El log va en modo `"a"`: un log es un
historial y ahí sí queremos que crezca.

---

## Instalación y uso

Hace falta **Python 3.8 o superior** y nada más: el proyecto usa solo la
biblioteca estándar (`csv`, `json`, `logging`, `os`, `datetime`, `urllib`).

Desde la raíz del proyecto:

```bash
python --version          # verificar que sea 3.8+
python src/main.py        # corre el pipeline completo
```

La primera corrida necesita internet: descarga las 8 series y las deja en
`data/raw/`. A partir de ahí se puede trabajar sin conexión, reutilizando lo que
ya está en disco:

```bash
python src/main.py --sin-internet
```

Los tests:

```bash
python tests/test_transform.py
```

19 tests: 17 en verde y 2 salteados (son los del bonus, todavía sin escribir).
Cubren cada función del transform con datos de juguete: el pasaje de ancho a
largo, los casos borde de las divisiones (total cero o nulo), el ranking por
grupo y el left join.

---

## De dónde salen los datos

INDEC, vía la [API de Series de Tiempo](https://apis.datos.gob.ar/series/api/)
de datos.gob.ar. Dos datasets:

| Dataset | Qué trae |
|---|---|
| **357.1** | Exportaciones por provincia y país de destino, más el total provincial |
| **350.1** | Exportaciones por provincia y rubro (productos primarios, MOA, MOI, combustibles y energía) |

Unidad: **millones de dólares FOB**. Período: **1993–2024**. Los IDs de las
series están en `config.py`, no en el código: si mañana cambian, se toca la
configuración y no la lógica.

Cada provincia aporta 10 países más una categoría `Resto`, que agrupa todo lo
que no entra en su top 10. Conviene tenerlo presente al leer los rankings:
`Resto` aparece muchas veces como destino #1 justamente porque es un agregado,
no un país.

---

## Estructura

```
├── config.py                  IDs de series, rutas, mapeo de regiones, umbrales
├── src/
│   ├── extract.py             Descarga de la API -> data/raw/
│   ├── transform.py           Ancho a largo, columnas derivadas, join
│   ├── load.py                Quality checks + CSV, JSON y log
│   └── main.py                Orquesta E -> T -> L
├── tests/
│   └── test_transform.py      19 tests unitarios
├── data/
│   ├── raw/                   8 JSON crudos, uno por provincia y grupo
│   └── processed/             exportaciones_nea.csv + resumen.json
└── logs/                      ejecucion.log (detalle) y pipeline.log (historial)
```

---

## El dataset que produce

`data/processed/exportaciones_nea.csv` — 13 columnas en este orden:

| # | Columna | Tipo | Descripción |
|:---:|---|---|---|
| 1 | `anio` | int | Año de la observación (1993–2024) |
| 2 | `provincia` | str | Chaco, Corrientes, Formosa o Misiones |
| 3 | `destino` | str | País de destino (o Resto) |
| 4 | `region_destino` | str | Región geoeconómica del destino |
| 5 | `valor_musd` | float | Exportado a ese destino, en millones de USD |
| 6 | `total_provincia_musd` | float | Total exportado por la provincia ese año |
| 7 | `participacion_pct` | float | `valor / total * 100` |
| 8 | `var_interanual_pct` | float | Variación vs. el año anterior (nula en el primer año de cada serie) |
| 9 | `decada` | str | 1990s, 2000s, 2010s o 2020s |
| 10 | `ranking_destino` | int | Posición del destino ese año (1 = el mayor) |
| 11 | `es_top3` | bool | Si está entre los 3 principales |
| 12 | `rubro_principal` | str | Rubro más exportado ese año (del join) |
| 13 | `pp_participacion_pct` | float | % de productos primarios ese año (del join) |

```csv
anio,provincia,destino,region_destino,valor_musd,total_provincia_musd,participacion_pct,var_interanual_pct,decada,ranking_destino,es_top3,rubro_principal,pp_participacion_pct
2024,Chaco,China,Asia,110.93,401.74,27.61,46.36,2020s,1,True,Productos primarios,81.3
2024,Chaco,Brasil,Mercosur,18.12,401.74,4.51,30.45,2020s,6,False,Productos primarios,81.3
```

`data/processed/resumen.json` es la ficha técnica: fuente, unidad, período,
cantidad de filas, estadísticas de `valor_musd` y el resultado de los cinco
quality checks. La idea es que quien reciba el CSV pueda saber de dónde salió
sin abrirlo.

---

## Algo que encontré en los datos

Lo que más me llamó la atención es **cómo Chaco cambió de socio comercial**. En
1993 China era su décimo destino, con 0,31 millones de dólares: el 0,2 % de lo
que exportaba la provincia, prácticamente nada. Brasil, en cambio, era el
comprador natural, y en 1998 llegó a ser el destino #1 con el 36 % del total.
Diez años después las posiciones están dadas vuelta: en 2003 China ya es el
destino #2 con el 29 %, y de ese puesto no se mueve en las dos décadas
siguientes. En 2024 China compra 110,93 millones (27,6 %) y Brasil apenas 18,12
millones (4,5 %, cuarto puesto). No es que Brasil haya dejado de comprar —en
valor absoluto compra más o menos lo mismo que en 1993— sino que China creció
por encima. La columna `pp_participacion_pct` cuenta la otra mitad de la
historia: los productos primarios pasaron del 37 % de las exportaciones
chaqueñas en 1993 al 81 % en 2024. La provincia no solo cambió de comprador,
también se volvió más dependiente de vender materia prima sin procesar.

También apareció **un año claramente raro: Corrientes en 2020**. Sus
exportaciones totales saltaron de 217 millones en 2019 a 586 millones, el máximo
de toda la serie y casi el triple del año anterior, en pleno año de pandemia.
Todo el salto está en una sola celda: Brasil pasó de 25 a 399 millones, una
variación interanual de **+1.478 %**, y se quedó con el 68 % del total
provincial. Al año siguiente el total baja a 298 millones y Brasil vuelve a
niveles previos. Es el valor más alto de `valor_musd` de todo el dataset, y es
justo el tipo de caso que hace que valga la pena tener la columna
`var_interanual_pct`: sin ella el número pasa desapercibido dentro del total.
Antes de dar por buena una explicación habría que ir a la fuente —puede ser una
operación puntual de energía o de arroz, o un cambio en cómo el INDEC imputó esa
serie—, pero el pipeline al menos lo deja visible.

Un tercer detalle, más chico: en Formosa el rubro principal fue **combustibles y
energía** entre 1999 y 2010 —con los productos primarios cayendo al 8 % en
2000— y después desaparece del primer puesto. En 2024 los primarios explican el
87 % de sus exportaciones. Doce años de una historia distinta metidos en el
medio de la serie.

---

*Fuente: INDEC, vía el portal de datos abiertos del Estado argentino
(datos.gob.ar), datasets 357.1 y 350.1.*
