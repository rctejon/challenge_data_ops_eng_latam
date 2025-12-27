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

| Paso | Fase | Área | Tarea | Rama | Documentación |
|------|------|------|-------|------|---------------|
| 1.1 | 1 | Diseño | Decisiones técnicas y stack | `main` | README |
| 2.1 | 2 | Scaffold | Descarga del dataset | `feature/scaffold-dataset-download` | README |
| 2.2 | 2 | Scaffold | Profiling del dataset | `feature/scaffold-dataset-profile` | [`src/dataset/dataset_profile.ipynb`](src/dataset/dataset_profile.ipynb) |
| 3.1 | 3 | Q1 | Análisis TIME (notebook) | `feature/q1-notebook-time` | [`src/q1/q1.ipynb`](src/q1/q1.ipynb) |
| 3.2 | 3 | Q1 | Análisis MEMORY (notebook) | `feature/q1-notebook-memory` | [`src/q1/q1.ipynb`](src/q1/q1.ipynb) |
| 3.3 | 3 | Q1 | Implementación TIME | `feature/q1-time-impl` | [`src/q1/q1.md`](src/q1/q1.md) |
| 3.4 | 3 | Q1 | Implementación MEMORY | `feature/q1-memory-impl` | [`src/q1/q1.md`](src/q1/q1.md) |
| 3.5 | 3 | Q2 | Análisis TIME + MEMORY (notebook) | `feature/q2-notebook` | [`src/q2/q2.ipynb`](src/q2/q2.ipynb) |
| 3.6 | 3 | Q2 | Implementación TIME | `feature/q2-time-impl` | [`src/q2/q2.md`](src/q2/q2.md) |
| 3.7 | 3 | Q2 | Implementación MEMORY | `feature/q2-time-impl` | [`src/q2/q2.md`](src/q2/q2.md) |
| 3.8 | 3 | Q3 | Análisis TIME + MEMORY (notebook) | `feature/q3-notebook` | [`src/q3/q3.ipynb`](src/q3/q3.ipynb) |
| 3.9 | 3 | Q3 | Implementación TIME | `feature/q3-time-impl` | [`src/q3/q3.md`](src/q3/q3.md) |
| 3.10 | 3 | Q3 | Implementación MEMORY | `feature/q3-memory-impl` | [`src/q3/q3.md`](src/q3/q3.md) |
| 4.1 | 4 | Entrega | Documentación global | `docs/final` | README |
| 4.2 | 4 | Entrega | Release final | `main` | README |



---

## Setup y Ejecución

```bash
# 1. Instalar dependencias
pip install -r requirements.txt

# 2. Descargar dataset (~398 MB desde Google Drive)
python src/dataset/download_dataset.py

# 3. Profiling del dataset (opcional, recomendado)
python src/dataset/dataset_profile.py
```

---

## Proceso de Desarrollo y Documentación

Cada pregunta (Q1, Q2, Q3) sigue un proceso riguroso de experimentación, análisis y documentación:

### 1. Experimentación en Notebooks

Antes de implementar cualquier solución, se realiza **experimentación exhaustiva en Jupyter notebooks** para evaluar diferentes enfoques:

- **Exploración de múltiples implementaciones**: Se prueban diferentes bibliotecas (Polars, Pandas) y estrategias (in-memory, streaming, chunked processing)
- **Comparación de performance**: Benchmarks de tiempo de ejecución (con 3+ runs para capturar variabilidad)
- **Análisis de memoria**: Medición de consumo de RAM (RSS delta) para cada enfoque
- **Profiling detallado**: cProfile para identificar bottlenecks de tiempo
- **Verificación de correctitud**: Validación de que todos los enfoques producen resultados idénticos
- **Análisis de trade-offs**: Evaluación de cuándo usar cada estrategia según el tamaño del dataset y recursos disponibles

**Notebooks por pregunta:**
- [Q1 - Notebook de experimentación](src/q1/q1.ipynb) - 4 enfoques comparados (Polars TIME, Pandas TIME, Polars MEMORY, Pandas MEMORY)
- [Q2 - Notebook de experimentación](src/q2/q2.ipynb) - Análisis TIME vs MEMORY
- [Q3 - Notebook de experimentación](src/q3/q3.ipynb) - Análisis TIME vs MEMORY

### 2. Implementación de Scripts

Basándose en los resultados del notebook, se implementan **scripts productivos** con las mejores soluciones:

- **Función pura** (`q*_time.py`, `q*_memory.py`): Solo la función requerida por el challenge, sin logging ni benchmarking
- **Runner con profiling** (`q*_time_impl.py`, `q*_memory_impl.py`): Ejecución completa con benchmarking reproducible

### 3. Documentación Técnica

Cada pregunta incluye documentación detallada en formato Markdown:

**Documentación por pregunta:**
- [Q1 - Documentación y guía de ejecución](src/q1/q1.md)
- [Q2 - Documentación y guía de ejecución](src/q2/q2.md)
- [Q3 - Documentación y guía de ejecución](src/q3/q3.md)

**Cada documento incluye:**
- ✅ **Instrucciones de ejecución**: Comandos exactos para ejecutar implementaciones TIME y MEMORY
- ✅ **Análisis de profiling**: Cómo analizar los resultados con cProfile y memray
- ✅ **Métricas de performance**: Tiempos de ejecución y uso de memoria medidos
- ✅ **Resultados esperados**: Top 10 outputs validados
- ✅ **Trade-offs documentados**: Cuándo usar cada enfoque según el caso de uso

### 4. Ejecución de Soluciones

Para ejecutar las implementaciones finales de cada pregunta:

```bash
# Q1 - Top 10 fechas con más tweets
python src/q1/q1_time_impl.py    # Optimizado por velocidad (~0.3s, ~129 MB)
python src/q1/q1_memory_impl.py  # Optimizado por memoria (~3.4s, ~7 MB)

# Q2 - Top 10 emojis más usados
python src/q2/q2_time_impl.py    # Optimizado por velocidad
python src/q2/q2_memory_impl.py  # Optimizado por memoria

# Q3 - Top 10 usuarios más influyentes
python src/q3/q3_time_impl.py    # Optimizado por velocidad
python src/q3/q3_memory_impl.py  # Optimizado por memoria
```

Cada script genera automáticamente archivos de profiling (`.prof` para cProfile, `.bin` para memray) que pueden analizarse posteriormente.

---