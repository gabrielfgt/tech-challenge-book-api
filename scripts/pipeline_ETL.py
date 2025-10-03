"""Orquestrador simples do pipeline de dados.

Etapas:
 1. Data Cleaning
 2. Data Transformation
 3. Feature Engineering
 4. Exploratory Data Analysis

Gera um relatório consolidado em CSV com status e duração por etapa.
"""
from __future__ import annotations

import time
from dataclasses import dataclass
from typing import List, Dict
from pathlib import Path
import traceback

# Utilitários comuns
from scripts.utils import ReportGenerator

from scripts.configs import (
    STATISTICS_DATA_DIR,
)
from scripts.cleaning import DataCleaner
from scripts.transformation import DataTransformer
from scripts.features import FeatureEngineer
from scripts.exploratory import ExploratoryDataAnalysis

PIPELINE_REPORT_FILENAME = "pipeline_execution_report.csv"

@dataclass
class StepResult:
    name: str
    status: str
    duration_sec: float
    artifact: str | None = None
    error: str | None = None

class PipelineRunner:
    def __init__(self) -> None:
        STATISTICS_DATA_DIR.mkdir(parents=True, exist_ok=True)
        self.results: List[StepResult] = []

    def _run_step(self, name: str, func, artifact_extractor=None) -> None:
        start = time.time()
        status = "success"
        artifact_repr = None
        error_msg = None
        try:
            output = func()
            if artifact_extractor:
                artifact_repr = artifact_extractor(output)
            else:
                artifact_repr = str(output)[:300]
        except Exception as e:  # pragma: no cover (erro inesperado)
            status = "failed"
            error_msg = f"{e.__class__.__name__}: {e}"
            traceback.print_exc()
        end = time.time()
        self.results.append(
            StepResult(
                name=name,
                status=status,
                duration_sec=round(end - start, 3),
                artifact=artifact_repr,
                error=error_msg,
            )
        )

    def run(self) -> Path:
        # 1. Cleaning
        cleaner = DataCleaner()
        self._run_step("data_cleaning", cleaner.run, artifact_extractor=lambda p: str(p))

        # 2. Transformation (só se anterior OK)
        if self.results[-1].status == "success":
            transformer = DataTransformer()
            self._run_step("data_transformation", transformer.run, artifact_extractor=lambda p: str(p))
        else:
            self.results.append(StepResult("data_transformation", "skipped", 0.0))

        # 3. Feature Engineering
        if self.results[-1].status == "success":
            fe = FeatureEngineer()
            self._run_step(
                "feature_engineering",
                fe.run,
                artifact_extractor=lambda d: ",".join(f"{k}={v}" for k, v in d.items()),
            )
        else:
            self.results.append(StepResult("feature_engineering", "skipped", 0.0))

        # 4. EDA
        if self.results[-1].status == "success":
            eda = ExploratoryDataAnalysis()
            self._run_step(
                "eda",
                eda.run,
                artifact_extractor=lambda d: ",".join(f"{k}={v}" for k, v in d.items()),
            )
        else:
            self.results.append(StepResult("eda", "skipped", 0.0))

        # Salva relatório consolidado usando ReportGenerator
        report_rows: List[Dict[str, str | float]] = []
        for r in self.results:
            report_rows.append(
                {
                    "step": r.name,
                    "status": r.status,
                    "duration_sec": r.duration_sec,
                    "artifact": r.artifact or "",
                    "error": r.error or "",
                }
            )
        
        out_path = ReportGenerator.save_report(
            report_rows,
            STATISTICS_DATA_DIR / PIPELINE_REPORT_FILENAME,
            "execucao_pipeline"
        )
        return out_path


def main() -> None:
    runner = PipelineRunner()
    report_path = runner.run()
    print(f"Pipeline concluída. Relatório: {report_path}")
    for res in runner.results:
        print(f" - {res.name}: {res.status} ({res.duration_sec}s)")


if __name__ == "__main__":  # pragma: no cover
    main()
