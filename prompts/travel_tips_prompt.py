#!/usr/bin/env python3
"""
Travel Tips & Advice AI Summarization Prompt
Focuses on actionable travel advice, planning insights, and practical tips
"""

def create_travel_tips_prompt(posts_text, category, total_posts, time_filter='weekly'):
    """
    Create travel tips focused prompt for comprehensive analysis
    
    Structure:
    1. Overview: 2-3 sentence high-level snapshot
    2. Key Themes: 3-4 recurring/important topics with specifics
    3. Travel Implications: Actionable insights for travelers
    """
    
    # Category-specific analysis focus
    category_guidance = {
        'General Travel Advice': {
            'focus': 'travel planning, logistics, visa requirements, and general travel tips',
            'themes': 'destination planning, travel documents, booking strategies, safety advice',
            'implications': 'planning approaches, booking timing, documentation needs, safety protocols'
        },
        'Solo Travel': {
            'focus': 'solo travel safety, social aspects, confidence building, and solo-specific challenges',
            'themes': 'safety strategies, meeting people, overcoming fear, solo travel benefits',
            'implications': 'safety measures, social strategies, confidence building, solo trip planning'
        },
        'Budget Travel': {
            'focus': 'cost-saving strategies, budget accommodations, cheap transportation, and money management',
            'themes': 'budget accommodations, transportation deals, food savings, free activities',
            'implications': 'cost reduction techniques, budget planning, money-saving tools, affordable alternatives'
        }
    }
    
    category_info = category_guidance.get(category, {
        'focus': 'travel advice, planning strategies, and practical travel tips',
        'themes': 'travel planning, destination insights, practical advice, traveler experiences',
        'implications': 'travel strategies, planning optimization, practical recommendations, trip enhancement'
    })
    
    prompt = f"""You are a travel expert creating a comprehensive summary of Reddit travel discussions for experienced travelers, travel planners, and adventure seekers.

**CRITICAL: KEEP ENTIRE RESPONSE UNDER 325 WORDS - THIS IS A HARD LIMIT**

**ANALYSIS SCOPE:**
- Category: {category}
- Time Period: {time_filter.title()} data
- Total Posts Analyzed: Top {total_posts} highest-ranked posts
- Focus: Actionable travel insights and practical advice

**CATEGORY FOCUS:**
This category typically centers on {category_info['focus']}. Pay special attention to {category_info['themes']}.

**CONTENT TO ANALYZE:**
{posts_text}

**REQUIRED OUTPUT STRUCTURE (MAXIMUM 325 WORDS TOTAL):**

**Overview** (40-50 words max)
Write 2-3 concise sentences capturing broad themes, traveler concerns, and discussion diversity. High-level snapshot only.

**Key Themes** (170-200 words max)
Identify the 3 most important travel topics using numbered format. For each theme:
- State theme clearly with specific examples
- 1-2 sentences with SPECIFIC details: exact destinations, costs, timeframes, visa requirements, accommodation types
- Be extremely specific - avoid generic terms
- Include concrete examples and actionable details
- Format each theme as: "1. [First theme content] 2. [Second theme content]" etc.

**Travel Implications** (80-100 words max)  
Provide 2 NEW strategic insights NOT repeating Key Themes using numbered format. Each point 1-2 sentences:
- Specific planning recommendations with timeframes
- Concrete safety or budget strategies with amounts/thresholds
- Tactical travel advice based on analysis
- Strategic frameworks with clear actions
- Format each implication as: "1. [First implication] 2. [Second implication]" etc.

CRITICAL FORMATTING RULES:
- Do NOT include any sub-bullets, context sections, or engagement scores
- Do NOT repeat specific destinations, costs, or detailed advice from Key Themes
- Each Key Theme should be a clean paragraph without sub-sections
- Be extremely specific - always include exact destinations, costs, timeframes, visa details, and accommodation types when available
- Avoid generic language like "various destinations," "different approaches," "some travelers," "many countries"
- Travel Implications should provide higher-level strategic guidance answering "What should I DO differently when traveling?"

IMPORTANT: Use exactly the 3 section headers above (Overview, Key Themes, Travel Implications) with no additional formatting. Keep Overview concise (2-3 sentences), make Key Themes highly specific with concrete details, and ensure Travel Implications offers unique strategic value.

STRICT WORD LIMIT: Keep the entire summary under 325 words total - this is a hard limit. NEVER end mid-sentence. If a sentence would push you over the 325-word limit, DELETE that entire sentence and end cleanly with the previous complete sentence. Prioritize the most important and specific information. Be concise and eliminate unnecessary words while maintaining specificity.

SENTENCE COMPLETION RULE: Absolutely NO mid-sentence cutoffs. If you're near the 325-word limit:
1. Count words carefully as you write
2. If starting a new sentence would exceed 325 words, DO NOT write it at all
3. End cleanly with the last complete sentence
4. Better to have fewer complete numbered points than incomplete ones
5. REMOVE any sentence that would cause you to exceed the limit"""
    
    return prompt

def test_travel_tips_prompt():
    """Test the travel tips prompt"""
    
    # Sample travel tips content
    sample_text = """
Title: Solo Female Travel to Thailand - Safety Tips Needed
Content: Planning my first solo trip to Thailand for 3 weeks in March. Worried about safety, especially in Bangkok and islands. Budget is $2000 total. Any specific areas to avoid? Best way to get around? Should I book hostels in advance or find them on arrival?

Title: European Rail Pass Worth It? 3 Week Itinerary Help
Content: Planning 3 weeks in Europe: London-Paris-Amsterdam-Berlin-Prague-Vienna-Budapest. Eurail pass costs $650 for 3 weeks. Is it worth it vs individual tickets? Also need advice on best hostels under $30/night in each city. Traveling in June.

Title: Budget Backpacking Southeast Asia - $1500 for 6 weeks?
Content: Is $1500 realistic for 6 weeks covering Thailand, Vietnam, Cambodia, Laos? Planning to stay in $5-10 hostels, eat street food, use buses/trains. Biggest concerns are visa costs and border crossings. Any money-saving tips for activities?

Title: Visa Requirements Confusion - Schengen Area Question
Content: US citizen planning 4 months in Europe. Understand Schengen is 90 days max. Can I do 90 days Schengen, then 30 days UK, then back to Schengen for another 90? Or does the clock restart? Planning to work remotely so need reliable wifi.

Title: Last-Minute Flight Deals - Best Apps and Websites?
Content: Looking for spontaneous travel options. Which apps actually find good last-minute deals? Tried Hopper, Skyscanner, Google Flights. Willing to fly anywhere for under $400 from NYC. Prefer weeklong trips with flexible dates.

Title: Travel Insurance Worth It for 2-Week Japan Trip?
Content: Spending $4000 on Japan trip including flights. Travel insurance quotes around $200-300. Is it worth it? Biggest concerns are flight cancellations and medical emergencies. Already have health insurance but not sure about international coverage.
"""
    
    prompt = create_travel_tips_prompt(sample_text, "General Travel Advice", 6, "weekly")
    
    print("🧳 TRAVEL TIPS AI PROMPT:")
    print("=" * 80)
    print(prompt)
    print("=" * 80)

if __name__ == "__main__":
    test_travel_tips_prompt()