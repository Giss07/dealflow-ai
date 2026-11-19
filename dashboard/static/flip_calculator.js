/*
 * The owner's flip calculator — a line-for-line port of flip_calculator.py so
 * the Distressed Deals page can update Max offer and Profit as ARV / rehab are
 * edited. test_distressed_page.py runs this file under Node against the Python
 * version on thousands of random inputs; keep the two in step.
 *
 * 3-month hold: buying closing 1% of purchase plus a fixed wholesale fee
 * (paid at closing, NOT financed; the page sets DEFAULTS.wholesale_fee from the
 * server's WHOLESALE_FEE); loan = purchase + buying
 * closing + repairs at 12%/yr; property tax 1.2% of purchase per year;
 * insurance and utilities $80/mo each; selling = $100 staging + 1% of
 * purchase + 2% + 2% of ARV agents. MAO = highest purchase price leaving a
 * profit of 10% of ARV, rounded down to $100.
 */
(function (root) {
  const DEFAULTS = {
    hold_months: 3, buy_closing_pct: 0.01, reinstatement: 0, wholesale_fee: 15000, monthly_maintenance: 0,
    interest_rate: 0.12, points_pct: 0, tax_rate: 0.012, insurance_monthly: 80, utilities_monthly: 80,
    staging: 100, sell_closing_pct: 0.01, seller_credit_pct: 0, listing_agent_pct: 0.02,
    buyers_agent_pct: 0.02, target_profit_pct: 0.10,
  };

  // fee: optional wholesale fee for this deal (defaults to DEFAULTS.wholesale_fee).
  function flipCosts(purchase, arv, repairs, fee) {
    const c = DEFAULTS, months = c.hold_months;
    const wholesale = (fee == null) ? c.wholesale_fee : fee;
    const buyingClosing = purchase * c.buy_closing_pct;
    const acquisition = purchase + buyingClosing + c.reinstatement + wholesale;
    const renovation = repairs + c.monthly_maintenance * months;
    const loan = purchase + buyingClosing + repairs;          // wholesale fee is not financed
    const interest = loan * c.interest_rate * months / 12;
    const points = loan * c.points_pct;
    const taxes = purchase * c.tax_rate * months / 12;
    const holding = interest + points + taxes + c.insurance_monthly * months + c.utilities_monthly * months;
    const selling = c.staging + purchase * c.sell_closing_pct + arv * c.seller_credit_pct
                  + arv * c.listing_agent_pct + arv * c.buyers_agent_pct;
    const total = acquisition + renovation + holding + selling;
    return { acquisition, repairs: renovation, holding, selling, total_costs: total, profit: arv - total };
  }

  // Returns {mao, profit} or {mao: null, profit: null} when no price reaches the target.
  function maxAllowableOffer(arv, repairs, fee) {
    const target = arv * DEFAULTS.target_profit_pct;
    const p0 = flipCosts(0, arv, repairs, fee).profit;
    const p1 = flipCosts(1, arv, repairs, fee).profit;
    const slope = p0 - p1;
    if (slope <= 0 || p0 < target) return { mao: null, profit: null };
    const mao = Math.floor(((p0 - target) / slope) / 100) * 100;
    return { mao, profit: flipCosts(mao, arv, repairs, fee).profit };
  }

  // The owner's ARV rule — a port of flip_analysis.arv_calculation:
  // median comp (middle sold price; with an even count the one just below the
  // middle) + (subject sqft − its sqft) × its Zillow $/sqft (listed_ppsf, or
  // sold price ÷ sqft), half-up to $100, held between the lowest and highest
  // comp sold price. comps: [{address, sold_price, sqft, listed_ppsf}].
  // anchor: the Anchor Comp's address; it breaks a tie for the median sold price
  // (otherwise the tied comp closest in sqft to the subject wins).
  function arvFromComps(subjectSqft, comps, anchor) {
    if (!comps || !comps.length || !subjectSqft) return null;
    const ranked = [...comps].sort((a, b) => a.sold_price - b.sold_price);
    let mid = ranked[Math.floor((ranked.length - 1) / 2)];
    const tied = ranked.filter(c => c.sold_price === mid.sold_price);
    if (tied.length > 1) {
      const byAnchor = tied.filter(c => anchor && c.address === anchor);
      mid = byAnchor.length ? byAnchor[0]
          : tied.reduce((best, c) => Math.abs(c.sqft - subjectSqft) < Math.abs(best.sqft - subjectSqft) ? c : best);
    }
    const ppsf = mid.listed_ppsf || pyRound(mid.sold_price / mid.sqft);
    const diff = pyRound(subjectSqft - mid.sqft);
    const raw0 = diff * ppsf;
    const sizeAdj = Math.sign(raw0) * Math.floor(Math.abs(raw0) / 100 + 0.5) * 100;
    const raw = mid.sold_price + sizeAdj;
    const low = ranked[0].sold_price, high = ranked[ranked.length - 1].sold_price;
    const cap = raw > high ? 'highest' : raw < low ? 'lowest' : null;
    const arv = cap === 'highest' ? high : cap === 'lowest' ? low : raw;
    const fmt = n => '$' + Math.round(n).toLocaleString('en-US');
    const name = String(mid.address || '').split(',')[0].trim();
    let explain = `Median ${fmt(mid.sold_price)}${name ? ` (${name})` : ''} ${sizeAdj >= 0 ? '+' : '−'} size adj ${fmt(Math.abs(sizeAdj))} ` +
                  `(${Math.abs(diff).toLocaleString('en-US')} sqft × ${fmt(ppsf)}) = ${fmt(raw)}`;
    if (cap) explain += ` → capped at ${cap} comp ${fmt(arv)}`;
    return { arv: Math.round(arv), median_comp: mid.address, size_adj: sizeAdj, raw: Math.round(raw), cap, explain };
  }
  // Python's round(): halves go to the even neighbour (round(2.5) == 2).
  function pyRound(x) {
    const f = Math.floor(x), d = x - f;
    if (d > 0.5) return f + 1;
    if (d < 0.5) return f;
    return f % 2 === 0 ? f : f + 1;
  }

  const api = { DEFAULTS, flipCosts, maxAllowableOffer, arvFromComps };
  if (typeof module !== 'undefined' && module.exports) module.exports = api;
  else root.FlipCalc = api;
})(typeof window !== 'undefined' ? window : globalThis);
