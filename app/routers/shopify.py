from __future__ import annotations

from fastapi import APIRouter, Depends, Query, Request, status
from fastapi.responses import HTMLResponse, RedirectResponse
from sqlalchemy.ext.asyncio import AsyncSession

from app.services.database import get_session
from app.services.shopify_service import ShopifyService


router = APIRouter(tags=["shopify"])


@router.get(
    "/business/{business_id}/integrations/shopify/connect",
    status_code=status.HTTP_307_TEMPORARY_REDIRECT,
)
async def connect_shopify(
    business_id: int,
    shop: str = Query(..., min_length=3),
    return_to: str | None = Query(default=None),
    session: AsyncSession = Depends(get_session),
) -> RedirectResponse:
    auth_url = await ShopifyService(session=session).begin_oauth_install(
        business_id=business_id,
        shop_domain=shop,
        return_to=return_to,
    )
    return RedirectResponse(auth_url, status_code=status.HTTP_307_TEMPORARY_REDIRECT)


@router.get("/integrations/shopify/callback", status_code=status.HTTP_307_TEMPORARY_REDIRECT)
async def shopify_callback(
    request: Request,
    session: AsyncSession = Depends(get_session),
):
    redirect_target = await ShopifyService(session=session).handle_oauth_callback(
        dict(request.query_params)
    )
    await session.commit()
    if redirect_target.startswith("about:blank"):
        return HTMLResponse(
            """
            <html>
              <body>
                <script>
                  if (window.opener) {
                    window.opener.postMessage({ type: "shopify-oauth-complete" }, "*");
                  }
                  window.close();
                </script>
                <p>Shopify connection completed. You can close this window.</p>
              </body>
            </html>
            """
        )
    return RedirectResponse(redirect_target, status_code=status.HTTP_307_TEMPORARY_REDIRECT)


@router.post("/webhooks/shopify/orders/create", status_code=status.HTTP_200_OK)
async def shopify_orders_create(
    request: Request,
    session: AsyncSession = Depends(get_session),
) -> dict[str, str]:
    result = await ShopifyService(session=session).handle_orders_create(
        headers=request.headers,
        body=await request.body(),
    )
    await session.commit()
    return {"status": str(result["status"])}


@router.post("/webhooks/shopify/orders/updated", status_code=status.HTTP_200_OK)
async def shopify_orders_updated(
    request: Request,
    session: AsyncSession = Depends(get_session),
) -> dict[str, str]:
    result = await ShopifyService(session=session).handle_orders_updated(
        headers=request.headers,
        body=await request.body(),
    )
    await session.commit()
    return {"status": str(result["status"])}


@router.post("/webhooks/shopify/app/uninstalled", status_code=status.HTTP_200_OK)
async def shopify_app_uninstalled(
    request: Request,
    session: AsyncSession = Depends(get_session),
) -> dict[str, str]:
    result = await ShopifyService(session=session).handle_app_uninstalled(
        headers=request.headers,
        body=await request.body(),
    )
    await session.commit()
    return {"status": str(result["status"])}
