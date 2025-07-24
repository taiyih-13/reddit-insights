#!/usr/bin/env python3
"""
Finance-optimized prompt templates for AI summarization
"""

def create_finance_prompt(posts_text, category, total_posts):
    """
    Create a sophisticated finance-focused prompt for AI summarization
    """
    
    # Category-specific instructions
    category_guidance = {
        'Analysis & Education': """
Focus heavily on:
- Investment theses and analytical frameworks
- Valuation metrics, ratios, and financial data
- Risk assessments and potential catalysts
- Technical analysis insights
- Educational value for investors
""",
        'Market News & Politics': """
Focus heavily on:
- Market impact and implications
- Policy changes affecting investments
- Economic indicators and trends
- Sector-specific effects
- Timeline and probability of events
""",
        'Personal Trading Stories': """
Focus heavily on:
- Trading strategies and lessons learned
- Risk management insights
- Emotional/psychological aspects of trading
- Performance metrics and outcomes
- Actionable takeaways for other traders
""",
        'Questions & Help': """
Focus heavily on:
- Common investor concerns and solutions
- Educational opportunities
- Best practices being discussed
- Resource recommendations
- Community consensus and advice
""",
        'Community Discussion': """
Focus heavily on:
- Market sentiment and mood
- Popular topics and trends
- Community predictions and expectations
- Debate topics and differing viewpoints
- Emerging opportunities or concerns
""",
        'Memes & Entertainment': """
Focus heavily on:
- Market sentiment through humor
- Popular stocks being memed
- Community mood and reactions
- Cultural aspects of investing
- Underlying serious topics within the humor
"""
    }
    
    specific_guidance = category_guidance.get(category, "")
    
    prompt = f"""You are a financial analyst summarizing Reddit discussions for serious investors and traders. 

**CATEGORY:** {category} ({total_posts} posts analyzed)

**YOUR TASK:** Create a professional, actionable summary that helps investors make informed decisions.

{specific_guidance}

**CONTENT TO ANALYZE:**
{posts_text}

**REQUIREMENTS:**
- Write 150-200 words in a professional, analytical tone
- Structure with clear headers: **Market Sentiment**, **Key Themes**, **Notable Mentions**, **Investment Implications**
- Use financial terminology appropriately (bullish/bearish, support/resistance, catalysts, etc.)
- Highlight specific stocks, prices, targets, and timeframes mentioned
- Include risk factors and contrarian viewpoints when present
- Focus on actionable insights that traders/investors can use
- Quantify sentiment when possible (e.g., "predominantly bullish" vs "mixed sentiment")

**INVESTMENT SUMMARY:**"""
    
    return prompt

def test_finance_prompt():
    """Test the new finance-focused prompt"""
    
    # Sample finance content
    sample_text = """
Title: Tesla Q3 Earnings Beat - Bullish on $300 Target
Content: Tesla crushed earnings with 20% revenue growth. Production ramp looking strong. Price target raised to $300. Key catalyst is robotaxi announcement next month.

Title: Fed Rate Cut Implications for Tech Stocks
Content: 50bps cut likely means rotation into growth stocks. QQQ could see 10-15% upside. Watch for sector rotation from value to growth.

Title: My NVDA Loss - Risk Management Lesson
Content: Lost 40% on NVDA calls by not setting stop losses. Learned to never risk more than 2% per trade. Emotional trading kills returns.
"""
    
    prompt = create_finance_prompt(sample_text, "Analysis & Education", 3)
    
    print("🧪 NEW FINANCE-FOCUSED PROMPT:")
    print("=" * 60)
    print(prompt)
    print("=" * 60)

if __name__ == "__main__":
    test_finance_prompt()