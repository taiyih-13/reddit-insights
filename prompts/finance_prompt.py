#!/usr/bin/env python3
"""
Comprehensive Finance-Focused AI Summarization Prompt
Leverages full 131k token capacity for complete category analysis
"""

def create_comprehensive_finance_prompt(posts_text, category, total_posts, time_filter='weekly'):
    """
    Create comprehensive finance-focused prompt using full token capacity
    
    Structure:
    1. Overview: 2-3 sentence high-level snapshot
    2. Key Themes: 3-4 recurring/important topics
    3. Notable Mentions: All stocks/tickers mentioned
    4. Investment Implications: Actionable insights for traders/investors
    """
    
    # Category-specific analysis focus
    category_guidance = {
        'Analysis & Education': {
            'focus': 'analytical frameworks, valuation metrics, technical analysis, and educational insights',
            'themes': 'investment theses, risk assessments, financial modeling, market analysis techniques',
            'implications': 'research approaches, valuation methods, analytical tools, learning opportunities'
        },
        'Market News & Politics': {
            'focus': 'market impact, policy implications, economic indicators, and sector effects',
            'themes': 'regulatory changes, economic trends, political developments, market reactions',
            'implications': 'policy timing, sector rotations, macro positioning, risk factors'
        },
        'Personal Trading Stories': {
            'focus': 'trading strategies, performance outcomes, lessons learned, and risk management',
            'themes': 'strategy effectiveness, emotional trading, risk management, position sizing',
            'implications': 'trading psychology, strategy refinement, risk controls, performance optimization'
        },
        'Questions & Help': {
            'focus': 'investor concerns, educational needs, best practices, and community advice',
            'themes': 'common mistakes, learning resources, investment approaches, decision-making',
            'implications': 'beginner guidance, advanced techniques, tool recommendations, educational priorities'
        },
        'Community Discussion': {
            'focus': 'market sentiment, popular trends, community predictions, and collective wisdom',
            'themes': 'sentiment shifts, trending topics, crowd psychology, emerging opportunities',
            'implications': 'sentiment indicators, contrarian opportunities, crowd behavior, timing signals'
        },
        'Memes & Entertainment': {
            'focus': 'sentiment through humor, popular stocks, community mood, and cultural trends',
            'themes': 'market humor, viral stocks, community reactions, retail sentiment',
            'implications': 'retail sentiment gauge, meme stock potential, market psychology, cultural indicators'
        }
    }
    
    category_info = category_guidance.get(category, {
        'focus': 'financial discussions and market-related content',
        'themes': 'investment topics, market trends, trading strategies, financial insights',
        'implications': 'investment opportunities, risk factors, market positioning, strategic decisions'
    })
    
    prompt = f"""You are a senior financial analyst creating a comprehensive summary of Reddit finance discussions for institutional investors, professional traders, and serious retail investors.

**CRITICAL: KEEP ENTIRE RESPONSE UNDER 325 WORDS - THIS IS A HARD LIMIT**

**ANALYSIS SCOPE:**
- Category: {category}
- Time Period: {time_filter.title()} data
- Total Posts Analyzed: Top {total_posts} highest-ranked posts
- Focus: Quality over quantity analysis

**CATEGORY FOCUS:**
This category typically centers on {category_info['focus']}. Pay special attention to {category_info['themes']}.

**CONTENT TO ANALYZE:**
{posts_text}

**REQUIRED OUTPUT STRUCTURE (MAXIMUM 325 WORDS TOTAL):**

**Overview** (40-50 words max)
Write 2-3 concise sentences capturing broad themes, tone, and diversity. High-level snapshot only.

**Key Themes** (170-200 words max)
Identify the 3 most important topics using numbered format. For each theme:
- State theme clearly with specific examples
- 1-2 sentences with SPECIFIC details: exact tickers, prices, dates, percentages, names
- Be extremely specific - avoid generic terms
- Include concrete examples and numbers
- Format each theme as: "1. [First theme content] 2. [Second theme content]" etc.

**Investment Implications** (80-100 words max)  
Provide 2 NEW strategic insights NOT repeating Key Themes using numbered format. Each point 1-2 sentences:
- Specific portfolio positioning with percentages
- Concrete risk management with thresholds/timeframes  
- Tactical positioning based on analysis
- Strategic frameworks with clear actions
- Format each implication as: "1. [First implication] 2. [Second implication]" etc.

CRITICAL FORMATTING RULES:
- Do NOT include any sub-bullets, context sections, or engagement scores
- Do NOT repeat specific stocks, companies, or detailed analysis from Key Themes
- Each Key Theme should be a clean paragraph without sub-sections
- Be extremely specific - always include exact tickers, prices, dates, percentages, and names when available
- Avoid generic language like "various stocks," "different approaches," "some users," "many companies"
- Investment Implications should provide higher-level strategic guidance answering "What should I DO differently?"

IMPORTANT: Use exactly the 3 section headers above (Overview, Key Themes, Investment Implications) with no additional formatting. Keep Overview concise (2-3 sentences), make Key Themes highly specific with concrete details, and ensure Investment Implications offers unique strategic value.

STRICT WORD LIMIT: Keep the entire summary under 325 words total - this is a hard limit. NEVER end mid-sentence. If a sentence would push you over the 325-word limit, DELETE that entire sentence and end cleanly with the previous complete sentence. Prioritize the most important and specific information. Be concise and eliminate unnecessary words while maintaining specificity.

SENTENCE COMPLETION RULE: Absolutely NO mid-sentence cutoffs. If you're near the 325-word limit:
1. Count words carefully as you write
2. If starting a new sentence would exceed 325 words, DO NOT write it at all
3. End cleanly with the last complete sentence
4. Better to have fewer complete numbered points than incomplete ones
5. REMOVE any sentence that would cause you to exceed the limit"""
    
    return prompt

def test_comprehensive_prompt():
    """Test the new comprehensive finance prompt"""
    
    # Sample comprehensive finance content
    sample_text = """
Title: Tesla Q3 Earnings Analysis - Path to $300
Content: Deep dive into TSLA Q3 numbers. Revenue beat by 8%, margins expanding due to production efficiencies. Key catalyst is robotaxi unveiling Q1 2024. Fair value using DCF shows $280-320 range. Risk factors include regulatory delays and competition from Chinese EVs. Setting $300 price target with 12-month horizon.

Title: Fed Rate Cut Impact on Growth vs Value Rotation  
Content: 50bps cut probability now 80% according to fed futures. Historical analysis shows growth stocks (QQQ) outperform value (VTV) by average 15% in 6 months post-cut. Tech names like NVDA, GOOGL, META likely beneficiaries. However, watch for yield curve steepening which could favor financials (XLF). Position sizing: 60% growth, 30% value, 10% cash.

Title: My NVDA Options Disaster - Risk Management Lessons
Content: Lost $25k on NVDA 0DTE calls last week. Violated every rule: no stop loss, position size too large (15% of portfolio), emotional FOMO trading. Learned harsh lesson about position sizing and time decay. New rules: max 2% risk per trade, always set stops, no 0DTE unless scalping with tight risk. Could have bought shares instead and been profitable.

Title: Best Resources for Learning Options Trading?
Content: New to options, overwhelmed by Greeks and strategies. Heard about Tasty Trade and Options Profit Calculator. Also considering Investopedia courses. What's the best path for beginners? Should I start with covered calls or CSPs? Portfolio is mostly index funds (VTI, VXUS) worth $50k.

Title: Market Sentiment Shift - Bear Rally or New Bull?
Content: SPY broke above 200-day MA with strong volume. However, RSI showing divergence and VIX still elevated. Reddit sentiment seems euphoric which historically marks tops. DXY strength concerning for earnings. My take: bear market rally, not sustainable. Expecting retest of lows by year-end. Playing defensive with TLT and gold miners (GDX).

Title: Why I'm Loading Up on Chinese Stocks (BABA, JD)
Content: Chinese ADRs at multi-year lows despite strong fundamentals. BABA trading at 8x P/E vs 25x for Amazon. JD logistics business undervalued. Risk is delisting but probability low given revenue dependence. Allocation: 5% BABA, 3% JD, 2% KWEB for diversification. Time horizon 2-3 years for regulatory clarity.
"""
    
    prompt = create_comprehensive_finance_prompt(sample_text, "Analysis & Education", 6, "weekly")
    
    print("🚀 COMPREHENSIVE FINANCE-FOCUSED PROMPT (131k Token Capacity):")
    print("=" * 80)
    print(prompt)
    print("=" * 80)

if __name__ == "__main__":
    test_comprehensive_prompt()