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

  function flipCosts(purchase, arv, repairs) {
    const c = DEFAULTS, months = c.hold_months;
    const buyingClosing = purchase * c.buy_closing_pct;
    const acquisition = purchase + buyingClosing + c.reinstatement + c.wholesale_fee;
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
  function maxAllowableOffer(arv, repairs) {
    const target = arv * DEFAULTS.target_profit_pct;
    const p0 = flipCosts(0, arv, repairs).profit;
    const p1 = flipCosts(1, arv, repairs).profit;
    const slope = p0 - p1;
    if (slope <= 0 || p0 < target) return { mao: null, profit: null };
    const mao = Math.floor(((p0 - target) / slope) / 100) * 100;
    return { mao, profit: flipCosts(mao, arv, repairs).profit };
  }

  const api = { DEFAULTS, flipCosts, maxAllowableOffer };
  if (typeof module !== 'undefined' && module.exports) module.exports = api;
  else root.FlipCalc = api;
})(typeof window !== 'undefined' ? window : globalThis);
