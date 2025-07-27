#!/usr/bin/env python3
"""
Regional Travel AI Summarization Prompt
Focuses on destination-specific advice, cultural insights, and regional travel patterns
"""

def create_regional_travel_prompt(posts_text, category, total_posts, time_filter='weekly'):
    """
    Create regional travel focused prompt for comprehensive analysis
    
    Structure:
    1. Overview: 2-3 sentence high-level snapshot
    2. Key Themes: 3-4 recurring/important topics with specifics
    3. Regional Implications: Actionable insights for regional travelers
    """
    
    # Category-specific analysis focus
    category_guidance = {
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
        }
    }
    
    category_info = category_guidance.get(category, {
        'focus': 'regional travel experiences, destination insights, and cultural considerations',
        'themes': 'destination planning, cultural insights, practical logistics, regional specialties',
        'implications': 'regional strategies, cultural adaptation, logistical optimization, experience enhancement'
    })
    
    prompt = f"""You are a regional travel specialist creating a comprehensive summary of Reddit regional travel discussions for destination-focused travelers, cultural explorers, and international adventurers.

**CRITICAL: KEEP ENTIRE RESPONSE UNDER 325 WORDS - THIS IS A HARD LIMIT**

**ANALYSIS SCOPE:**
- Region: {category}
- Time Period: {time_filter.title()} data
- Total Posts Analyzed: Top {total_posts} highest-ranked posts
- Focus: Destination-specific insights and regional travel strategies

**REGIONAL FOCUS:**
This region typically involves {category_info['focus']}. Pay special attention to {category_info['themes']}.

**CONTENT TO ANALYZE:**
{posts_text}

**REQUIRED OUTPUT STRUCTURE (MAXIMUM 325 WORDS TOTAL):**

**Overview** (40-50 words max)
Write 2-3 concise sentences capturing broad themes, regional concerns, and destination diversity. High-level snapshot only.

**Key Themes** (170-200 words max)
Identify the 3 most important regional topics using numbered format. For each theme:
- State theme clearly with specific examples
- 1-2 sentences with SPECIFIC details: exact destinations, costs, visa requirements, timeframes, cultural practices
- Be extremely specific - avoid generic terms
- Include concrete examples and regional details
- Format each theme as: "1. [First theme content] 2. [Second theme content]" etc.

**Regional Implications** (80-100 words max)  
Provide 2 NEW strategic insights NOT repeating Key Themes using numbered format. Each point 1-2 sentences:
- Specific regional planning recommendations with timeframes
- Concrete cultural or logistical strategies with details
- Tactical destination advice based on analysis
- Strategic frameworks with clear actions
- Format each implication as: "1. [First implication] 2. [Second implication]" etc.

CRITICAL FORMATTING RULES:
- Do NOT include any sub-bullets, context sections, or engagement scores
- Do NOT repeat specific destinations, costs, or detailed advice from Key Themes
- Each Key Theme should be a clean paragraph without sub-sections
- Be extremely specific - always include exact destinations, costs, visa details, timeframes, and cultural practices when available
- Avoid generic language like "various countries," "different approaches," "some regions," "many destinations"
- Regional Implications should provide higher-level strategic guidance answering "What should I DO differently when traveling to this region?"

IMPORTANT: Use exactly the 3 section headers above (Overview, Key Themes, Regional Implications) with no additional formatting. Keep Overview concise (2-3 sentences), make Key Themes highly specific with concrete details, and ensure Regional Implications offers unique strategic value.

STRICT WORD LIMIT: Keep the entire summary under 325 words total - this is a hard limit. NEVER end mid-sentence. If a sentence would push you over the 325-word limit, DELETE that entire sentence and end cleanly with the previous complete sentence. Prioritize the most important and specific information. Be concise and eliminate unnecessary words while maintaining specificity.

SENTENCE COMPLETION RULE: Absolutely NO mid-sentence cutoffs. If you're near the 325-word limit:
1. Count words carefully as you write
2. If starting a new sentence would exceed 325 words, DO NOT write it at all
3. End cleanly with the last complete sentence
4. Better to have fewer complete numbered points than incomplete ones
5. REMOVE any sentence that would cause you to exceed the limit"""
    
    return prompt

def test_regional_travel_prompt():
    """Test the regional travel prompt"""
    
    # Sample regional travel content
    sample_text = """
Title: Two Weeks in Japan - Tokyo to Kyoto Rail Pass Worth It?
Content: Planning 14-day trip: Tokyo (5 days), Kyoto (4 days), Osaka (2 days), Hiroshima (2 days), Mount Fuji (1 day). JR Pass costs $435 for 14 days. Individual tickets might be cheaper? Also need advice on ryokan reservations - hearing they book up months in advance for cherry blossom season.

Title: Schengen Visa Confusion - 90 Days Rule Clarification
Content: US citizen wanting to spend 4 months in Europe. Understand Schengen allows 90 days in 180-day period. Can I do 90 days Schengen, then 30 days UK/Ireland, then return for another 90? Planning remote work from Portugal, France, and Germany. Need reliable internet for video calls.

Title: Southeast Asia Route - 8 Weeks Budget Breakdown
Content: Planning Thailand (2 weeks) → Vietnam (2 weeks) → Cambodia (1 week) → Laos (1 week) → back to Thailand (2 weeks). Budget $3000 total. Biggest expenses: flights ($800), visas ($200), accommodation ($800 averaging $15/night), food ($600), activities ($400), transport ($200). Realistic?

Title: India Travel - Delhi Belly Prevention and Cultural Dos/Don'ts
Content: First-time India travel, landing Delhi, going Rajasthan circuit: Jaipur-Udaipur-Jodhpur. Main concerns: food safety, cultural sensitivity, bargaining etiquette. Heard filtered water essential, avoid street food first week. Should I book guided tours or independent travel? Budget $2000 for 3 weeks.

Title: Morocco Desert Tour - Sahara Experience From Marrakech
Content: Booked 3-day Sahara desert tour from Marrakech for $250 including camel trekking, desert camp, all meals. Departs to Merzouga via Atlas Mountains. Need packing advice for temperature swings - heard it's 90F day, 40F night in March. What to expect culturally?

Title: Australia Working Holiday Visa - Best Cities for Backpackers
Content: Got WHV approval for Australia. Considering Melbourne vs Sydney vs Brisbane for base. Need city with good hostel scene, job opportunities (hospitality/farm work), and affordable cost of living. Budget $8000 for first 3 months. When's best time to arrive?
"""
    
    prompt = create_regional_travel_prompt(sample_text, "Asia", 6, "weekly")
    
    print("🌍 REGIONAL TRAVEL AI PROMPT:")
    print("=" * 80)
    print(prompt)
    print("=" * 80)

if __name__ == "__main__":
    test_regional_travel_prompt()