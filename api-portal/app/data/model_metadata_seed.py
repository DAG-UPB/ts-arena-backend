"""One-shot seed data for ``models.model_info`` discovery metadata.

Keyed by the model's ``readable_id``. Applied at api-portal startup via
``apply_metadata_seed`` in :mod:`app.main`. Updates only fields that are
currently NULL (``COALESCE``), so any user-set values are preserved and
the seed is safe to re-run on every boot.

This bridges the gap between the frontend's curated family content
(``ts-arena-frontend/src/content/models/families/*.tsx``) and the new
database columns introduced in ticket #43, so the Resources panel on
``/models/[modelId]`` has data to render without anyone re-registering
the existing reference models.
"""

from typing import Dict, Optional, TypedDict


class ModelSeed(TypedDict, total=False):
    paper_url: Optional[str]
    arxiv_id: Optional[str]
    repo_url: Optional[str]
    website_url: Optional[str]


# Family-level links applied to every version in the family. Sources
# verified against arXiv, HF model cards, and official org repos.
MODEL_METADATA_SEED: Dict[str, ModelSeed] = {
    # ---- Chronos (Amazon Science) ----
    "chronos-bolt-tiny": {
        "paper_url": "https://arxiv.org/abs/2403.07815",
        "arxiv_id": "2403.07815",
        "repo_url": "https://github.com/amazon-science/chronos-forecasting",
        "website_url": "https://www.amazon.science/blog/introducing-chronos-2-from-univariate-to-universal-forecasting",
    },
    "chronos-bolt-mini": {
        "paper_url": "https://arxiv.org/abs/2403.07815",
        "arxiv_id": "2403.07815",
        "repo_url": "https://github.com/amazon-science/chronos-forecasting",
        "website_url": "https://www.amazon.science/blog/introducing-chronos-2-from-univariate-to-universal-forecasting",
    },
    "chronos-bolt-small": {
        "paper_url": "https://arxiv.org/abs/2403.07815",
        "arxiv_id": "2403.07815",
        "repo_url": "https://github.com/amazon-science/chronos-forecasting",
        "website_url": "https://www.amazon.science/blog/introducing-chronos-2-from-univariate-to-universal-forecasting",
    },
    "chronos-bolt-base": {
        "paper_url": "https://arxiv.org/abs/2403.07815",
        "arxiv_id": "2403.07815",
        "repo_url": "https://github.com/amazon-science/chronos-forecasting",
        "website_url": "https://www.amazon.science/blog/introducing-chronos-2-from-univariate-to-universal-forecasting",
    },
    "chronos-2": {
        # Chronos-2 has its own dedicated paper.
        "paper_url": "https://arxiv.org/abs/2510.15821",
        "arxiv_id": "2510.15821",
        "repo_url": "https://github.com/amazon-science/chronos-forecasting",
        "website_url": "https://www.amazon.science/blog/introducing-chronos-2-from-univariate-to-universal-forecasting",
    },

    # ---- FlowState (IBM Research) ----
    "flowstate": {
        "paper_url": "https://arxiv.org/abs/2508.05287",
        "arxiv_id": "2508.05287",
        "repo_url": "https://huggingface.co/ibm-research/flowstate",
        "website_url": "https://research.ibm.com/publications/flowstate-sampling-rate-invariant-time-series-foundation-model-with-dynamic-forecasting-horizons",
    },

    # ---- Moirai (Salesforce AI Research) ----
    "moirai-small": {
        "paper_url": "https://arxiv.org/abs/2402.02592",
        "arxiv_id": "2402.02592",
        "repo_url": "https://github.com/SalesforceAIResearch/uni2ts",
        "website_url": "https://www.salesforce.com/blog/moirai/",
    },
    "moirai-base-model": {
        "paper_url": "https://arxiv.org/abs/2402.02592",
        "arxiv_id": "2402.02592",
        "repo_url": "https://github.com/SalesforceAIResearch/uni2ts",
        "website_url": "https://www.salesforce.com/blog/moirai/",
    },
    "moirai-large": {
        "paper_url": "https://arxiv.org/abs/2402.02592",
        "arxiv_id": "2402.02592",
        "repo_url": "https://github.com/SalesforceAIResearch/uni2ts",
        "website_url": "https://www.salesforce.com/blog/moirai/",
    },
    "moirai-2-small": {
        "paper_url": "https://arxiv.org/abs/2402.02592",
        "arxiv_id": "2402.02592",
        "repo_url": "https://github.com/SalesforceAIResearch/uni2ts",
        "website_url": "https://www.salesforce.com/blog/moirai/",
    },

    # ---- MOMENT (Auton Lab, CMU) ----
    "moment-small": {
        "paper_url": "https://arxiv.org/abs/2402.03885",
        "arxiv_id": "2402.03885",
        "repo_url": "https://github.com/moment-timeseries-foundation-model/moment",
        "website_url": "https://huggingface.co/AutonLab/MOMENT-1-large",
    },
    "moment-base-model": {
        "paper_url": "https://arxiv.org/abs/2402.03885",
        "arxiv_id": "2402.03885",
        "repo_url": "https://github.com/moment-timeseries-foundation-model/moment",
        "website_url": "https://huggingface.co/AutonLab/MOMENT-1-large",
    },
    "moment-large": {
        "paper_url": "https://arxiv.org/abs/2402.03885",
        "arxiv_id": "2402.03885",
        "repo_url": "https://github.com/moment-timeseries-foundation-model/moment",
        "website_url": "https://huggingface.co/AutonLab/MOMENT-1-large",
    },

    # ---- Sundial (THUML, Tsinghua) ----
    "sundial-128m": {
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
    "time-moe-50m": {
        "paper_url": "https://arxiv.org/abs/2409.16040",
        "arxiv_id": "2409.16040",
        "repo_url": "https://github.com/Time-MoE/Time-MoE",
        "website_url": "https://huggingface.co/Maple728/TimeMoE-200M",
    },
    "time-moe-200m": {
        "paper_url": "https://arxiv.org/abs/2409.16040",
        "arxiv_id": "2409.16040",
        "repo_url": "https://github.com/Time-MoE/Time-MoE",
        "website_url": "https://huggingface.co/Maple728/TimeMoE-200M",
    },

    # ---- TimesFM (Google Research) ----
    "timesfm-2.0-500m": {
        "paper_url": "https://arxiv.org/abs/2310.10688",
        "arxiv_id": "2310.10688",
        "repo_url": "https://github.com/google-research/timesfm",
        "website_url": "https://research.google/blog/a-decoder-only-foundation-model-for-time-series-forecasting/",
    },
    "timesfm-2.5-200m": {
        "paper_url": "https://arxiv.org/abs/2310.10688",
        "arxiv_id": "2310.10688",
        "repo_url": "https://github.com/google-research/timesfm",
        "website_url": "https://research.google/blog/a-decoder-only-foundation-model-for-time-series-forecasting/",
    },

    # ---- TinyTimeMixer / TTM (IBM Granite) ----
    "tinytimemixer-r1-512-96": {
        "paper_url": "https://arxiv.org/abs/2401.03955",
        "arxiv_id": "2401.03955",
        "repo_url": "https://github.com/ibm-granite/granite-tsfm",
        "website_url": "https://huggingface.co/ibm-granite/granite-timeseries-ttm-r1",
    },
    "tinytimemixer-r1-1024-96": {
        "paper_url": "https://arxiv.org/abs/2401.03955",
        "arxiv_id": "2401.03955",
        "repo_url": "https://github.com/ibm-granite/granite-tsfm",
        "website_url": "https://huggingface.co/ibm-granite/granite-timeseries-ttm-r1",
    },
    "tinytimemixer-r2-512-96": {
        "paper_url": "https://arxiv.org/abs/2401.03955",
        "arxiv_id": "2401.03955",
        "repo_url": "https://github.com/ibm-granite/granite-tsfm",
        "website_url": "https://huggingface.co/ibm-granite/granite-timeseries-ttm-r2",
    },
    "tinytimemixer-r2-1024-96": {
        "paper_url": "https://arxiv.org/abs/2401.03955",
        "arxiv_id": "2401.03955",
        "repo_url": "https://github.com/ibm-granite/granite-tsfm",
        "website_url": "https://huggingface.co/ibm-granite/granite-timeseries-ttm-r2",
    },

    # ---- TiRex (NX-AI) ----
    "tirex": {
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
    "visiontspp-base": {
        "paper_url": "https://arxiv.org/abs/2508.04379",
        "arxiv_id": "2508.04379",
        "repo_url": "https://github.com/HALF111/VisionTSpp",
        "website_url": "https://huggingface.co/Lefei/VisionTSpp",
    },
    "visiontspp-large": {
        "paper_url": "https://arxiv.org/abs/2508.04379",
        "arxiv_id": "2508.04379",
        "repo_url": "https://github.com/HALF111/VisionTSpp",
        "website_url": "https://huggingface.co/Lefei/VisionTSpp",
    },

    # Statistical baselines have no canonical paper / repo / website —
    # intentionally absent from the seed.
}
