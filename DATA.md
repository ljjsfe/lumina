# Data Sources

Argos does not bundle any benchmark data. Download the datasets below and place them in `data/` before running evaluations.

---

## KDD Cup 2026 — Phase 1 Demo

**Source:** [KDD Cup 2026 Competition Page](https://www.kaggle.com/competitions/kdd-cup-2026)

**Download:** Phase 1 demo data package from the competition page (requires Kaggle account).

**Expected layout after extraction:**

```
data/demo/
├── input/
│   └── task_<id>/
│       ├── task.json          # task_id, difficulty, question
│       └── context/           # csv/, db/, json/, doc/, knowledge.md
└── output/
    └── task_<id>/
        └── gold.csv           # ground truth answer
```

**Dev set:** 50 tasks (`task_11`, `task_86`, `task_145`, `task_163`, `task_169`, `task_173`, `task_200`, `task_228`, `task_240`, `task_243`, `task_246`, `task_254`, `task_256`, `task_292`, `task_316`, `task_321`, `task_330`, `task_337`, `task_352`, `task_355`, `task_367`, `task_374`, `task_379`, `task_385`, `task_388`, `task_394`, `task_404`, `task_408`, `task_414`, `task_418`, `task_420`, `task_422`, `task_423`, `task_428`, `task_431`, `task_432`, `task_436`, `task_446`, `task_452`, `task_456`, `task_459`, `task_461`, `task_467`, `task_469`, `task_473`, `task_479`, `task_481`, `task_485`, `task_490`, `task_494`)

**Config reference:** `config.yaml` → `eval.kdd_gold_dir: data/demo`

---

## DABstep — Adyen Payment Benchmark

**Source:** [DABstep on Hugging Face](https://huggingface.co/datasets/adyen/DABstep)

```bash
# Install Hugging Face CLI first if needed
pip install huggingface_hub

# Download
huggingface-cli download adyen/DABstep --repo-type dataset --local-dir data/dabstep
```

**Expected layout after download:**

```
data/dabstep/
├── context/
│   ├── payments.csv
│   ├── fees.json
│   ├── manual.md
│   ├── merchant_data.json
│   ├── merchant_category_codes.csv
│   ├── acquirer_countries.csv
│   └── payments-readme.md
└── dev_tasks.json             # 10 dev tasks with questions + answers
```

**Dev set:** 10 tasks (in `dev_tasks.json`)

**Config reference:** `config.yaml` → `eval.dabstep_dir: data/dabstep`

---

## Quick check

```bash
python main.py --task data/demo/input/task_11 --output results/smoke_test
```
