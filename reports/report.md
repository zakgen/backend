# AI Reply Evaluation Report

## Run Summary
- Generated at: 2026-04-03T15:24:55.729394+00:00
- Backend endpoint: http://localhost:8000
- Business ID: 5
- Judge model: gpt-4.1-mini
- Total queries: 30
- Pass rate: 90.0%
- Average overall score: 4.7/5
- Average latency: 14779.17 ms

## Average Scores
- Relevance: 4.77/5
- Accuracy: 4.67/5
- Language Match: 4.93/5
- Completeness: 4.13/5
- Tone: 4.77/5
- Hallucination Risk: 4.93/5

## Breakdown by Language
- English: pass rate 100.0%, average score 4.87/5
- French: pass rate 90.0%, average score 4.67/5
- Darija: pass rate 80.0%, average score 4.57/5

## Breakdown by Topic
- Products: pass rate 77.78%, average score 4.55/5
- Profile: pass rate 88.89%, average score 4.61/5
- Delivery: pass rate 100.0%, average score 4.78/5
- Orders: pass rate 100.0%, average score 4.97/5

## Top 3 Best Responses
- [da_delivery_007] 5.0/5 | wach kayn delivery l Rabat? chhal frais w ch7al kaywsel? -> Ahlen! Kayn delivery l Rabat b 35 MAD, w kaywsel f 24 7tta l 48 sa3a. Ila bghiti t3rf akthar, ana hna.
- [da_orders_010] 5.0/5 | هادشي طول بزاف. بغيت نلغي commande و نبعث شكاية، ندير شنو؟ -> Tadبير dyal les commandes kaydirouh support, machi مباشرة من هاد الشات. Ila bghiti suivi, modification, annulation, ولا شكاية، تواصل m3a support. Contacts dyal support: WhatsApp: +212661234567; Phone: +212522450980; Email: support@atlasgadgethub.ma
- [da_orders_009] 5.0/5 | درت commande من قبل، واش نقدر نبدلها قبل ما تخرج مع livreur؟ -> Tadبير dyal les commandes kaydirouh support, machi مباشرة من هاد الشات. Ila bghiti suivi, modification, annulation, ولا شكاية، تواصل m3a support. Contacts dyal support: WhatsApp: +212661234567; Phone: +212522450980; Email: support@atlasgadgethub.ma

## Top 3 Worst Responses
- [da_products_002] 3.83/5 | بغيت شي حاجة مزيانة من catégorie Audio, شنو عندكم دابا؟ -> عندنا مجموعة مزيانة من أجهزة الصوت بحال سماعات، مكبرات صوت، وملحقات صوتية أخرى. واش كتفضل شي نوع معين أو ماركة؟
- [da_profile_006] 3.33/5 | ila chrit produit w ma3jbnich, واش عندي 7 أيام باش نرجعو؟ -> سلام، بالنسبة لسياسة الإرجاع عندنا، ما عنديش معلومات دقيقة على مدة 7 أيام للإرجاع. باش نعاونك مزيان، من الأفضل تتواصل مع الدعم ديالنا عبر WhatsApp +212661234567 أو الإيميل support@atlasgadgethub.ma.
- [fr_products_003] 3.33/5 | Vous vendez aussi des iPhone reconditionnés ou seulement les produits affichés chez Atlas Gadget Hub ? -> Notre adresse est 27 Rue Al Massira, Maarif, Casablanca, Morocco. Phone: +212522450980

## Failure Patterns
- missing_whatsapp_contact: 2
- missing_return_policy_details: 2
- missing_comparative_detail: 1
- missing_return_policy_info: 1
- missing_delivery_details: 1
- partial_answer: 1
- missing_specific_products: 1
- vague_product_info: 1
- missing_product_info: 1
- out_of_scope_not_handled: 1
- irrelevant_response: 1
- minor_inconsistency: 1
- partial_completeness: 1
- incomplete_detail: 1
- tone_not_fully_aligned: 1
- missing_product_details: 1
- vague_response: 1
- language_mismatch: 1
- missing_language_match_detail: 1
- unsupported_claim: 1
- incomplete_answer: 1

## Recommendations
- The system is stable overall; focus next on raising answer richness while keeping the same grounding quality.
