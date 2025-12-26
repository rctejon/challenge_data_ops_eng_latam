# Data Engineer Challenge – Implementation Plan

Este repositorio contiene la solución al Data Engineer Challenge, estructurada siguiendo un enfoque profesional de ingeniería de datos, con énfasis en reproducibilidad, profiling temprano, tradeoffs explícitos entre tiempo y memoria, y una organización clara del trabajo.

El desarrollo se divide en 4 fases, desde el diseño y análisis del dataset hasta la entrega final.

---

## Visión general del enfoque

- El dataset se analiza y perfila antes de implementar cualquier solución.
- Cada problema (q1, q2, q3) se resuelve con dos enfoques:
  - Optimizado por tiempo de ejecución.
  - Optimizado por uso de memoria.
- El profiling (tiempo y memoria) es parte del proceso de diseño, no solo una validación final.
- El código está organizado por pregunta, con notebooks explicativos, scripts reproducibles y documentación técnica separada.
- Se sigue una variante clara de GitFlow durante todo el desarrollo.

---

## Estructura del repositorio

```

.
├── .gitignore
├── README.md
├── requirements.txt
└── src
├── q1/
│   ├── q1.ipynb
│   ├── q1_time.py
│   ├── q1_memory.py
│   ├── q1_time_impl.py
│   ├── q1_memory_impl.py
│   └── q1.md
│
├── q2/
│   ├── q2.ipynb
│   ├── q2_time.py
│   ├── q2_memory.py
│   ├── q2_time_impl.py
│   ├── q2_memory_impl.py
│   └── q2.md
│
├── q3/
│   ├── q3.ipynb
│   ├── q3_time.py
│   ├── q3_memory.py
│   ├── q3_time_impl.py
│   ├── q3_memory_impl.py
│   └── q3.md
│
└── dataset/
├── download_dataset.py
├── dataset_profile.ipynb
└── dataset_profile.py

```

Notas:
- Los archivos `q*_time.py` y `q*_memory.py` contienen únicamente la función solicitada por el challenge.
- Los archivos `*_impl.py` son runners completos usados para profiling y reportes reproducibles.
- Cada pregunta tiene un archivo `qk.md` donde se documentan decisiones, análisis y resultados finales.
- El dataset descargado y los outputs de profiling no se versionan (ver `.gitignore`).

---

## Fases del desarrollo

### Fase 1 – Diseño y decisiones técnicas

Objetivo: definir el stack, herramientas y enfoques antes de escribir código.

Incluye:
- Uso de Python 3.11.
- `polars` para enfoques optimizados en tiempo.
- Streaming con `orjson` para enfoques optimizados en memoria.
- Uso combinado de `cProfile`, `memory-profiler` y `memray`.
- Definición de GitFlow y estructura del repositorio.

---

### Fase 2 – Planificación

Objetivo: dividir el trabajo en subtareas claras y ordenadas, minimizando riesgos técnicos.

Incluye:
- Scaffold completo del proyecto.
- Archivo `.gitignore` desde el inicio.
- Descarga reproducible del dataset.
- Profiling exhaustivo del dataset antes de resolver los problemas.
- Definición de un flujo fijo de 5 subtareas por pregunta.
- Integración del profiling como parte del proceso.

---

### Fase 3 – Implementación

Objetivo: implementar soluciones correctas, eficientes y medibles.

Incluye:
- Análisis en notebook por pregunta (tiempo y memoria).
- Implementaciones finales en scripts limpios.
- Profiling detallado:
  - `cProfile` para tiempo de ejecución.
  - `memory-profiler` y `memray` para uso de memoria.
- Manejo explícito de errores y casos borde.

---

### Fase 4 – Entrega

Objetivo: dejar el repositorio listo para evaluación.

Incluye:
- Documentación técnica final por pregunta.
- Comparación clara entre enfoques de tiempo y memoria.
- Supuestos y posibles mejoras futuras.
- Merge final a la rama `main`.
- Envío del link del repositorio según las instrucciones del challenge.

---

## Tabla de tareas por fase, rama y documentación

| Fase | Área | Tarea | Rama | Documentación |
|------|------|-------|------|---------------|
| 1 | Diseño | Decisiones técnicas y stack | `main` | README |
| 2 | Scaffold | Descarga del dataset | `feature/scaffold-dataset-download` | README |
| 2 | Scaffold | Profiling del dataset | `feature/scaffold-dataset-profile` | `src/dataset/dataset_profile.ipynb` |
| 3 | Q1 | Análisis TIME (notebook) | `feature/q1-notebook-time` | `src/q1/q1.ipynb` |
| 3 | Q1 | Análisis MEMORY (notebook) | `feature/q1-notebook-memory` | `src/q1/q1.ipynb` |
| 3 | Q1 | Implementación TIME | `feature/q1-time-impl` | `src/q1/q1.md` |
| 3 | Q1 | Implementación MEMORY | `feature/q1-memory-impl` | `src/q1/q1.md` |
| 3 | Q1 | Documentación final | `docs/q1` | `src/q1/q1.md` |
| 3 | Q3 | Flujo completo Q3 | `feature/q3-*`, `docs/q3` | `src/q3/q3.md` |
| 3 | Q2 | Flujo completo Q2 | `feature/q2-*`, `docs/q2` | `src/q2/q2.md` |
| 4 | Entrega | Documentación global | `docs/final` | README |
| 4 | Entrega | Release final | `main` | README |

---

## Setup y Ejecución

```bash
# 1. Instalar dependencias
pip install -r requirements.txt

# 2. Descargar dataset (~398 MB desde Google Drive)
python src/dataset/download_dataset.py
```


---