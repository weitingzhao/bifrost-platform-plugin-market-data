"""Placeholder analytics routes — implementations land in later waves."""

from __future__ import annotations

from fastapi import APIRouter
from fastapi.responses import JSONResponse

router = APIRouter(prefix="/analytics", tags=["analytics"])

_NOT_IMPLEMENTED = JSONResponse(
    status_code=501,
    content={"detail": "Not implemented"},
)


@router.get("/max-pain")
def max_pain() -> JSONResponse:
    return _NOT_IMPLEMENTED


@router.get("/atm-iv")
def atm_iv() -> JSONResponse:
    return _NOT_IMPLEMENTED


@router.get("/pcr")
def pcr() -> JSONResponse:
    return _NOT_IMPLEMENTED


@router.get("/iv-percentile")
def iv_percentile() -> JSONResponse:
    return _NOT_IMPLEMENTED
