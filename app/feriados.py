"""
Feriados nacionais e pontos facultativos do Brasil.
Fonte: Lei nº 9.093/1995 e decretos presidenciais anuais.

Feriados móveis são calculados automaticamente a partir da data da Páscoa.
"""

from __future__ import annotations

from datetime import date, timedelta

FERIADO_NACIONAL = "feriado nacional"
PONTO_FACULTATIVO = "ponto facultativo"


def _easter(year: int) -> date:
    """Calcula a data da Páscoa pelo algoritmo de Butcher (Gregoriano)."""
    a = year % 19
    b, c = divmod(year, 100)
    d, e = divmod(b, 4)
    f = (b + 8) // 25
    g = (b - f + 1) // 3
    h = (19 * a + b - d - g + 15) % 30
    i, k = divmod(c, 4)
    l = (32 + 2 * e + 2 * i - h - k) % 7
    m = (a + 11 * h + 22 * l) // 451
    month = (h + l - 7 * m + 114) // 31
    day = (h + l - 7 * m + 114) % 31 + 1
    return date(year, month, day)


# Pontos facultativos extras específicos por ano (pontes, decretos pontuais).
# Formato: {ano: [(mes, dia, nome, tipo), ...]}
_EXTRAS_POR_ANO: dict[int, list[tuple[int, int, str, str]]] = {
    2019: [(4, 22, "Ponte Tiradentes", PONTO_FACULTATIVO)],
    2022: [(4, 22, "Ponte Tiradentes", PONTO_FACULTATIVO)],
    2023: [(12, 29, "Ponto Facultativo", PONTO_FACULTATIVO)],
    2024: [(5, 31, "Ponte Corpus Christi", PONTO_FACULTATIVO)],
    2026: [
        (4, 20, "Ponte Tiradentes", PONTO_FACULTATIVO),
        (6,  5, "Corpus Christi+1", PONTO_FACULTATIVO),
    ],
}


def get_feriados(year: int) -> dict[int, list[tuple[int, str, str]]]:
    """
    Retorna todos os feriados e pontos facultativos do ano agrupados por mês.

    Returns
    -------
    dict[int, list[tuple[int, str, str]]]
        {mes: [(dia, nome, tipo), ...]} com as entradas ordenadas por dia.
        tipo ∈ {FERIADO_NACIONAL, PONTO_FACULTATIVO}

    Exemplos
    --------
    >>> feriados = get_feriados(2026)
    >>> feriados[4]   # Abril de 2026
    [(3, 'Paixão de Cristo', 'feriado nacional'),
     (20, 'Ponte Tiradentes', 'ponto facultativo'),
     (21, 'Tiradentes', 'feriado nacional')]
    """
    easter = _easter(year)

    # Datas móveis derivadas da Páscoa
    carnival_mon = easter - timedelta(days=48)   # Segunda de Carnaval
    carnival_tue = easter - timedelta(days=47)   # Terça de Carnaval
    ash_wed      = easter - timedelta(days=46)   # Quarta-Feira de Cinzas
    good_friday  = easter - timedelta(days=2)    # Paixão de Cristo
    corpus       = easter + timedelta(days=60)   # Corpus Christi

    entries: list[tuple[int, int, str, str]] = [
        # ── Feriados fixos ────────────────────────────────────────────────
        (1,  1,  "Confraternização Universal",    FERIADO_NACIONAL),
        (4,  21, "Tiradentes",                    FERIADO_NACIONAL),
        (5,  1,  "Dia Mundial do Trabalho",        FERIADO_NACIONAL),
        (9,  7,  "Independência do Brasil",        FERIADO_NACIONAL),
        (10, 12, "Nossa Senhora Aparecida",         FERIADO_NACIONAL),
        (10, 28, "Dia do Servidor Público",         PONTO_FACULTATIVO),
        (11, 2,  "Finados",                         FERIADO_NACIONAL),
        (11, 15, "Proclamação da República",        FERIADO_NACIONAL),
        (11, 20, "Consciência Negra",               FERIADO_NACIONAL),
        (12, 24, "Véspera do Natal",                PONTO_FACULTATIVO),
        (12, 25, "Natal",                           FERIADO_NACIONAL),
        (12, 31, "Véspera do Ano Novo",             PONTO_FACULTATIVO),
        # ── Feriados móveis (calculados a partir da Páscoa) ──────────────
        (carnival_mon.month, carnival_mon.day, "Carnaval",               PONTO_FACULTATIVO),
        (carnival_tue.month, carnival_tue.day, "Carnaval",               PONTO_FACULTATIVO),
        (ash_wed.month,      ash_wed.day,      "Quarta-Feira de Cinzas", PONTO_FACULTATIVO),
        (good_friday.month,  good_friday.day,  "Paixão de Cristo",       FERIADO_NACIONAL),
        (corpus.month,       corpus.day,       "Corpus Christi",         PONTO_FACULTATIVO),
    ]

    # Adiciona extras específicos do ano (pontes, decretos pontuais)
    for mes, dia, nome, tipo in _EXTRAS_POR_ANO.get(year, []):
        entries.append((mes, dia, nome, tipo))

    # Agrupa por mês, ordena por dia dentro de cada mês
    result: dict[int, list[tuple[int, str, str]]] = {}
    for mes, dia, nome, tipo in sorted(entries, key=lambda x: (x[0], x[1])):
        result.setdefault(mes, []).append((dia, nome, tipo))

    return result
