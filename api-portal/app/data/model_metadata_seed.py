"""One-shot seed data for ``models.model_info`` discovery metadata.

Keyed by the model's ``name`` field — the verbatim registration name
from ``ts-arena-models/challenge-uploads/src/config.json`` (e.g.
``"google/timesfm-2.0-500m-pytorch"``). We match on ``name`` rather than
``readable_id`` because ``generate_readable_id`` appends a random
``_<adj>_<animal><n>`` suffix, so the stored ``readable_id`` is
unpredictable. ``name`` is stable across registrations.

Applied at api-portal startup via ``apply_metadata_seed`` in
:mod:`app.main`. Updates only fields that are currently NULL
(``COALESCE``), so any user-set values are preserved and the seed is
safe to re-run on every boot.

This bridges the gap between the frontend's curated family content
(``ts-arena-frontend/src/content/models/families/*.tsx``) and the new
provenance columns, so the Resources panel on
``/models/[modelId]`` has data to render without anyone re-registering
the existing reference models.
"""

from typing import Dict, Optional, TypedDict


class ModelSeed(TypedDict, total=False):
    paper_url: Optional[str]
    arxiv_id: Optional[str]
    repo_url: Optional[str]
    website_url: Optional[str]


# Keyed by ``models.model_info.name``. Matches the ``name`` field in
# ts-arena-models/challenge-uploads/src/config.json verbatim.
MODEL_METADATA_SEED: Dict[str, ModelSeed] = {
    # ---- Chronos (Amazon Science) ----
    "amazon/chronos-bolt-tiny": {
        "paper_url": "https://arxiv.org/abs/2403.07815",
        "arxiv_id": "2403.07815",
        "repo_url": "https://github.com/amazon-science/chronos-forecasting",
        "website_url": "https://www.amazon.science/blog/introducing-chronos-2-from-univariate-to-universal-forecasting",
    },
    "amazon/chronos-bolt-mini": {
        "paper_url": "https://arxiv.org/abs/2403.07815",
        "arxiv_id": "2403.07815",
        "repo_url": "https://github.com/amazon-science/chronos-forecasting",
        "website_url": "https://www.amazon.science/blog/introducing-chronos-2-from-univariate-to-universal-forecasting",
    },
    "amazon/chronos-bolt-small": {
        "paper_url": "https://arxiv.org/abs/2403.07815",
        "arxiv_id": "2403.07815",
        "repo_url": "https://github.com/amazon-science/chronos-forecasting",
        "website_url": "https://www.amazon.science/blog/introducing-chronos-2-from-univariate-to-universal-forecasting",
    },
    "amazon/chronos-bolt-base": {
        "paper_url": "https://arxiv.org/abs/2403.07815",
        "arxiv_id": "2403.07815",
        "repo_url": "https://github.com/amazon-science/chronos-forecasting",
        "website_url": "https://www.amazon.science/blog/introducing-chronos-2-from-univariate-to-universal-forecasting",
    },
    "amazon/chronos-2": {
        # Chronos-2 has its own dedicated paper.
        "paper_url": "https://arxiv.org/abs/2510.15821",
        "arxiv_id": "2510.15821",
        "repo_url": "https://github.com/amazon-science/chronos-forecasting",
        "website_url": "https://www.amazon.science/blog/introducing-chronos-2-from-univariate-to-universal-forecasting",
    },

    # ---- FlowState (IBM Research) ----
    "ibm-research/flowstate": {
        "paper_url": "https://arxiv.org/abs/2508.05287",
        "arxiv_id": "2508.05287",
        "repo_url": "https://huggingface.co/ibm-research/flowstate",
        "website_url": "https://research.ibm.com/publications/flowstate-sampling-rate-invariant-time-series-foundation-model-with-dynamic-forecasting-horizons",
    },

    # ---- Moirai (Salesforce AI Research) ----
    "Salesforce/moirai-1.1-R-small": {
        "paper_url": "https://arxiv.org/abs/2402.02592",
        "arxiv_id": "2402.02592",
        "repo_url": "https://github.com/SalesforceAIResearch/uni2ts",
        "website_url": "https://www.salesforce.com/blog/moirai/",
    },
    "Salesforce/moirai-1.1-R-base": {
        "paper_url": "https://arxiv.org/abs/2402.02592",
        "arxiv_id": "2402.02592",
        "repo_url": "https://github.com/SalesforceAIResearch/uni2ts",
        "website_url": "https://www.salesforce.com/blog/moirai/",
    },
    "Salesforce/moirai-1.1-R-large": {
        "paper_url": "https://arxiv.org/abs/2402.02592",
        "arxiv_id": "2402.02592",
        "repo_url": "https://github.com/SalesforceAIResearch/uni2ts",
        "website_url": "https://www.salesforce.com/blog/moirai/",
    },
    "Salesforce/moirai-2.0-R-small": {
        "paper_url": "https://arxiv.org/abs/2402.02592",
        "arxiv_id": "2402.02592",
        "repo_url": "https://github.com/SalesforceAIResearch/uni2ts",
        "website_url": "https://www.salesforce.com/blog/moirai/",
    },

    # ---- MOMENT (Auton Lab, CMU) ----
    "AutonLab/MOMENT-1-small": {
        "paper_url": "https://arxiv.org/abs/2402.03885",
        "arxiv_id": "2402.03885",
        "repo_url": "https://github.com/moment-timeseries-foundation-model/moment",
        "website_url": "https://huggingface.co/AutonLab/MOMENT-1-large",
    },
    "AutonLab/MOMENT-1-base": {
        "paper_url": "https://arxiv.org/abs/2402.03885",
        "arxiv_id": "2402.03885",
        "repo_url": "https://github.com/moment-timeseries-foundation-model/moment",
        "website_url": "https://huggingface.co/AutonLab/MOMENT-1-large",
    },
    "AutonLab/MOMENT-1-large": {
        "paper_url": "https://arxiv.org/abs/2402.03885",
        "arxiv_id": "2402.03885",
        "repo_url": "https://github.com/moment-timeseries-foundation-model/moment",
        "website_url": "https://huggingface.co/AutonLab/MOMENT-1-large",
    },

    # ---- Sundial (THUML, Tsinghua) ----
    "thuml/sundial-base-128m": {
        "paper_url": "https://arxiv.org/abs/2502.00816",
        "arxiv_id": "2502.00816",
        "repo_url": "https://github.com/thuml/Sundial",
        "website_url": "https://huggingface.co/thuml/sundial-base-128m",
    },

    # ---- TabPFN-TS (Prior Labs / Hutter group) ----
    "tabpfn-ts": {
        "paper_url": "https://arxiv.org/abs/2501.02945",
        "arxiv_id": "2501.02945",
        "repo_url": "https://github.com/PriorLabs/tabpfn-time-series",
        "website_url": "https://priorlabs.ai/",
    },

    # ---- Time-MoE ----
    "Maple728/TimeMoE-50M": {
        "paper_url": "https://arxiv.org/abs/2409.16040",
        "arxiv_id": "2409.16040",
        "repo_url": "https://github.com/Time-MoE/Time-MoE",
        "website_url": "https://huggingface.co/Maple728/TimeMoE-200M",
    },
    "Maple728/TimeMoE-200M": {
        "paper_url": "https://arxiv.org/abs/2409.16040",
        "arxiv_id": "2409.16040",
        "repo_url": "https://github.com/Time-MoE/Time-MoE",
        "website_url": "https://huggingface.co/Maple728/TimeMoE-200M",
    },

    # ---- TimesFM (Google Research) ----
    "google/timesfm-2.0-500m-pytorch": {
        "paper_url": "https://arxiv.org/abs/2310.10688",
        "arxiv_id": "2310.10688",
        "repo_url": "https://github.com/google-research/timesfm",
        "website_url": "https://research.google/blog/a-decoder-only-foundation-model-for-time-series-forecasting/",
    },
    "google/timesfm-2.5-200m-pytorch": {
        "paper_url": "https://arxiv.org/abs/2310.10688",
        "arxiv_id": "2310.10688",
        "repo_url": "https://github.com/google-research/timesfm",
        "website_url": "https://research.google/blog/a-decoder-only-foundation-model-for-time-series-forecasting/",
    },

    # ---- TinyTimeMixer / TTM (IBM Granite) ----
    # r1 and r2 are two distinct HF model cards; the ctx/horizon variants
    # (512-96, 1024-96) re-use the same checkpoint name and are distinguished
    # by their `parameters` JSONB blob.
    "ibm-granite/granite-timeseries-ttm-r1": {
        "paper_url": "https://arxiv.org/abs/2401.03955",
        "arxiv_id": "2401.03955",
        "repo_url": "https://github.com/ibm-granite/granite-tsfm",
        "website_url": "https://huggingface.co/ibm-granite/granite-timeseries-ttm-r1",
    },
    "ibm-granite/granite-timeseries-ttm-r2": {
        "paper_url": "https://arxiv.org/abs/2401.03955",
        "arxiv_id": "2401.03955",
        "repo_url": "https://github.com/ibm-granite/granite-tsfm",
        "website_url": "https://huggingface.co/ibm-granite/granite-timeseries-ttm-r2",
    },

    # ---- TiRex (NX-AI) ----
    "NX-AI/TiRex": {
        "paper_url": "https://arxiv.org/abs/2505.23719",
        "arxiv_id": "2505.23719",
        "repo_url": "https://github.com/NX-AI/tirex",
        "website_url": "https://nx-ai.github.io/tirex/",
    },

    # ---- Toto (Datadog) ----
    "toto": {
        "paper_url": "https://arxiv.org/abs/2505.14766",
        "arxiv_id": "2505.14766",
        "repo_url": "https://github.com/DataDog/toto",
        "website_url": "https://huggingface.co/Datadog/Toto-Open-Base-1.0",
    },

    # ---- VisionTS++ ----
    "visiontspp_base.ckpt": {
        "paper_url": "https://arxiv.org/abs/2508.04379",
        "arxiv_id": "2508.04379",
        "repo_url": "https://github.com/HALF111/VisionTSpp",
        "website_url": "https://huggingface.co/Lefei/VisionTSpp",
    },
    "visiontspp_large.ckpt": {
        "paper_url": "https://arxiv.org/abs/2508.04379",
        "arxiv_id": "2508.04379",
        "repo_url": "https://github.com/HALF111/VisionTSpp",
        "website_url": "https://huggingface.co/Lefei/VisionTSpp",
    },

    # Statistical baselines have no canonical paper / repo / website —
    # intentionally absent from the seed.
}


# ---------------------------------------------------------------------------
# Family-level fallback. Applied for any row that didn't match by ``name``,
# so newly-registered variants of a known family get a sensible paper/repo
# auto-filled even before someone adds them to the per-name seed above.
# ---------------------------------------------------------------------------
MODEL_FAMILY_FALLBACK: Dict[str, ModelSeed] = {
    "chronos": {
        "paper_url": "https://arxiv.org/abs/2403.07815",
        "arxiv_id": "2403.07815",
        "repo_url": "https://github.com/amazon-science/chronos-forecasting",
        "website_url": "https://www.amazon.science/blog/introducing-chronos-2-from-univariate-to-universal-forecasting",
    },
    "flowstate": {
        "paper_url": "https://arxiv.org/abs/2508.05287",
        "arxiv_id": "2508.05287",
        "repo_url": "https://huggingface.co/ibm-research/flowstate",
        "website_url": "https://research.ibm.com/publications/flowstate-sampling-rate-invariant-time-series-foundation-model-with-dynamic-forecasting-horizons",
    },
    "moirai": {
        "paper_url": "https://arxiv.org/abs/2402.02592",
        "arxiv_id": "2402.02592",
        "repo_url": "https://github.com/SalesforceAIResearch/uni2ts",
        "website_url": "https://www.salesforce.com/blog/moirai/",
    },
    "moment": {
        "paper_url": "https://arxiv.org/abs/2402.03885",
        "arxiv_id": "2402.03885",
        "repo_url": "https://github.com/moment-timeseries-foundation-model/moment",
        "website_url": "https://huggingface.co/AutonLab/MOMENT-1-large",
    },
    "sundial": {
        "paper_url": "https://arxiv.org/abs/2502.00816",
        "arxiv_id": "2502.00816",
        "repo_url": "https://github.com/thuml/Sundial",
        "website_url": "https://huggingface.co/thuml/sundial-base-128m",
    },
    "tabpfn-ts": {
        "paper_url": "https://arxiv.org/abs/2501.02945",
        "arxiv_id": "2501.02945",
        "repo_url": "https://github.com/PriorLabs/tabpfn-time-series",
        "website_url": "https://priorlabs.ai/",
    },
    "time-moe": {
        "paper_url": "https://arxiv.org/abs/2409.16040",
        "arxiv_id": "2409.16040",
        "repo_url": "https://github.com/Time-MoE/Time-MoE",
        "website_url": "https://huggingface.co/Maple728/TimeMoE-200M",
    },
    "timesfm": {
        "paper_url": "https://arxiv.org/abs/2310.10688",
        "arxiv_id": "2310.10688",
        "repo_url": "https://github.com/google-research/timesfm",
        "website_url": "https://research.google/blog/a-decoder-only-foundation-model-for-time-series-forecasting/",
    },
    "tinytimemixer": {
        "paper_url": "https://arxiv.org/abs/2401.03955",
        "arxiv_id": "2401.03955",
        "repo_url": "https://github.com/ibm-granite/granite-tsfm",
        "website_url": "https://huggingface.co/ibm-granite/granite-timeseries-ttm-r2",
    },
    "tirex": {
        "paper_url": "https://arxiv.org/abs/2505.23719",
        "arxiv_id": "2505.23719",
        "repo_url": "https://github.com/NX-AI/tirex",
        "website_url": "https://nx-ai.github.io/tirex/",
    },
    "toto": {
        "paper_url": "https://arxiv.org/abs/2505.14766",
        "arxiv_id": "2505.14766",
        "repo_url": "https://github.com/DataDog/toto",
        "website_url": "https://huggingface.co/Datadog/Toto-Open-Base-1.0",
    },
    "visionts": {
        "paper_url": "https://arxiv.org/abs/2508.04379",
        "arxiv_id": "2508.04379",
        "repo_url": "https://github.com/HALF111/VisionTSpp",
        "website_url": "https://huggingface.co/Lefei/VisionTSpp",
    },
}
