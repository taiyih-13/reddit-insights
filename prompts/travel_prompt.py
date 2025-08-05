#!/usr/bin/env python3
"""
Unified Travel AI Summarization Prompt
Handles both regional travel and travel tips/advice content in one unified approach
"""

def create_travel_prompt(posts_text, category, total_posts, time_filter='weekly'):
    """
    Create unified travel prompt that handles both regional and tips categories
    
    Structure:
    1. Overview: 2-3 sentence high-level snapshot
    2. Key Themes: 3-4 recurring/important topics with specifics
    3. Travel Implications: Actionable insights for travelers
    """
    
    # Combined category-specific analysis focus
    category_guidance = {
        # Regional categories
        'Europe': {
            'focus': 'European travel logistics, Schengen regulations, cultural experiences, and transportation systems',
            'themes': 'visa requirements, rail travel, cultural etiquette, seasonal considerations',
            'implications': 'route planning, transportation booking, cultural preparation, timing optimization'
        },
        'Asia': {
            'focus': 'Asian travel experiences, visa processes, cultural adaptation, and budget considerations',
            'themes': 'visa complexity, cultural differences, food safety, transportation options',
            'implications': 'visa planning, cultural preparation, health precautions, budget optimization'
        },
        'Americas': {
            'focus': 'Americas travel covering North, Central, and South America with diverse experiences',
            'themes': 'border crossings, language barriers, safety considerations, currency variations',
            'implications': 'border preparation, language skills, safety protocols, currency planning'
        },
        'Oceania & Africa': {
            'focus': 'Oceania and African travel adventures, wildlife experiences, and unique logistics',
            'themes': 'wildlife encounters, health requirements, remote area access, seasonal travel',
            'implications': 'health preparation, seasonal planning, remote logistics, wildlife safety'
        },
        # Travel tips categories
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
        'focus': 'travel experiences, destination insights, and practical travel considerations',
        'themes': 'destination planning, cultural insights, practical logistics, traveler experiences',
        'implications': 'travel strategies, planning optimization, practical recommendations, trip enhancement'
    })
    
    # Determine if this is a regional or tips category for specialized messaging
    regional_categories = ['Europe', 'Asia', 'Americas', 'Oceania & Africa']
    is_regional = category in regional_categories
    
    specialist_type = "regional travel specialist" if is_regional else "travel expert"
    if is_regional:
        audience = "destination-focused travelers, cultural explorers, and international adventurers"
        focus_type = "destination-specific insights and regional travel strategies"
        section_name = "Regional Implications"
        section_guidance = "What should I DO differently when traveling to this region?"
        involves_centers = "involves"
        concerns_type = "regional concerns"
        diversity_type = "destination diversity"
        travel_type = "regional"
        details_type = "visa requirements, timeframes, cultural practices"
        regional_details = "regional details"
        planning_type = "regional planning"
        strategies_type = "cultural or logistical"
        strategy_details = "details"
        advice_type = "destination"
        language_warning = "various countries, different approaches, some regions, many destinations"
        accommodation_info = "visa details, timeframes, and cultural practices"
    else:
        audience = "experienced travelers, travel planners, and adventure seekers"
        focus_type = "actionable travel insights and practical advice"
        section_name = "Travel Implications"
        section_guidance = "What should I DO differently when traveling?"
        involves_centers = "centers on"
        concerns_type = "traveler concerns"
        diversity_type = "discussion diversity"
        travel_type = "travel"
        details_type = "timeframes, visa requirements, accommodation types"
        regional_details = "actionable details"
        planning_type = "planning"
        strategies_type = "safety or budget"
        strategy_details = "amounts/thresholds"
        advice_type = "travel"
        language_warning = "various destinations, different approaches, some travelers, many countries"
        accommodation_info = "timeframes, visa details, and accommodation types"
    
    prompt = f"""You are a {specialist_type} creating a comprehensive summary of Reddit travel discussions for {audience}.

**CRITICAL: KEEP ENTIRE RESPONSE UNDER 325 WORDS - THIS IS A HARD LIMIT**

**ANALYSIS SCOPE:**
- Category: {category}
- Time Period: {time_filter.title()} data
- Total Posts Analyzed: Top {total_posts} highest-ranked posts
- Focus: {focus_type}

**CATEGORY FOCUS:**
This category typically {involves_centers} {category_info['focus']}. Pay special attention to {category_info['themes']}.

**CONTENT TO ANALYZE:**
{posts_text}

**REQUIRED OUTPUT STRUCTURE (MAXIMUM 325 WORDS TOTAL):**

**Overview** (40-50 words max)
Write 2-3 concise sentences capturing broad themes, {concerns_type}, and {diversity_type}. High-level snapshot only.

**Key Themes** (170-200 words max)
Identify the 3 most important {travel_type} topics using numbered format. For each theme:
- State theme clearly with specific examples
- 1-2 sentences with SPECIFIC details: exact destinations, costs, {details_type}
- Be extremely specific - avoid generic terms
- Include concrete examples and {regional_details}
- Format each theme as: "1. [First theme content] 2. [Second theme content]" etc.

**{section_name}** (80-100 words max)  
Provide 2 NEW strategic insights NOT repeating Key Themes using numbered format. Each point 1-2 sentences:
- Specific {planning_type} recommendations with timeframes
- Concrete {strategies_type} strategies with {strategy_details}
- Tactical {advice_type} advice based on analysis
- Strategic frameworks with clear actions
- Format each implication as: "1. [First implication] 2. [Second implication]" etc.

CRITICAL FORMATTING RULES:
- Do NOT include any sub-bullets, context sections, or engagement scores
- Do NOT repeat specific destinations, costs, or detailed advice from Key Themes
- Each Key Theme should be a clean paragraph without sub-sections
- Be extremely specific - always include exact destinations, costs, {accommodation_info} when available
- Avoid generic language like "{language_warning}"
- {section_name} should provide higher-level strategic guidance answering "{section_guidance}"

IMPORTANT: Use exactly the 3 section headers above (Overview, Key Themes, {section_name}) with no additional formatting. Keep Overview concise (2-3 sentences), make Key Themes highly specific with concrete details, and ensure {section_name} offers unique strategic value.

STRICT WORD LIMIT: Keep the entire summary under 325 words total - this is a hard limit. NEVER end mid-sentence. If a sentence would push you over the 325-word limit, DELETE that entire sentence and end cleanly with the previous complete sentence. Prioritize the most important and specific information. Be concise and eliminate unnecessary words while maintaining specificity.

SENTENCE COMPLETION RULE: Absolutely NO mid-sentence cutoffs. If you're near the 325-word limit:
1. Count words carefully as you write
2. If starting a new sentence would exceed 325 words, DO NOT write it at all
3. End cleanly with the last complete sentence
4. Better to have fewer complete numbered points than incomplete ones
5. REMOVE any sentence that would cause you to exceed the limit"""
    
    return prompt

def test_travel_prompt():
    """Test the unified travel prompt with different categories"""
    
    # Sample travel content (mix of regional and tips)
    sample_text = """
Title: Two Weeks in Japan - Tokyo to Kyoto Rail Pass Worth It?
Content: Planning 14-day trip: Tokyo (5 days), Kyoto (4 days), Osaka (2 days), Hiroshima (2 days), Mount Fuji (1 day). JR Pass costs $435 for 14 days. Individual tickets might be cheaper? Also need advice on ryokan reservations - hearing they book up months in advance for cherry blossom season.

Title: Solo Female Travel to Thailand - Safety Tips Needed
Content: Planning my first solo trip to Thailand for 3 weeks in March. Worried about safety, especially in Bangkok and islands. Budget is $2000 total. Any specific areas to avoid? Best way to get around? Should I book hostels in advance or find them on arrival?

Title: European Rail Pass Worth It? 3 Week Itinerary Help
Content: Planning 3 weeks in Europe: London-Paris-Amsterdam-Berlin-Prague-Vienna-Budapest. Eurail pass costs $650 for 3 weeks. Is it worth it vs individual tickets? Also need advice on best hostels under $30/night in each city. Traveling in June.

Title: Budget Backpacking Southeast Asia - $1500 for 6 weeks?
Content: Is $1500 realistic for 6 weeks covering Thailand, Vietnam, Cambodia, Laos? Planning to stay in $5-10 hostels, eat street food, use buses/trains. Biggest concerns are visa costs and border crossings. Any money-saving tips for activities?

Title: Morocco Desert Tour - Sahara Experience From Marrakech
Content: Booked 3-day Sahara desert tour from Marrakech for $250 including camel trekking, desert camp, all meals. Departs to Merzouga via Atlas Mountains. Need packing advice for temperature swings - heard it's 90F day, 40F night in March. What to expect culturally?

Title: Australia Working Holiday Visa - Best Cities for Backpackers
Content: Got WHV approval for Australia. Considering Melbourne vs Sydney vs Brisbane for base. Need city with good hostel scene, job opportunities (hospitality/farm work), and affordable cost of living. Budget $8000 for first 3 months. When's best time to arrive?
"""
    
    # Test different categories
    test_categories = ["Asia", "Solo Travel", "Budget Travel", "General Travel Advice"]
    
    for category in test_categories:
        print(f"\n🌍 TESTING UNIFIED TRAVEL PROMPT - {category.upper()}:")
        print("=" * 80)
        prompt = create_travel_prompt(sample_text, category, 6, "weekly")
        print(prompt[:500] + "...")
        print("=" * 80)

if __name__ == "__main__":
    test_travel_prompt()