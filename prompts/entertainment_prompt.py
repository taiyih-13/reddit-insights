#!/usr/bin/env python3
"""
Entertainment-Focused AI Summarization Prompt
Creates structured recommendations based on Reddit entertainment discussions
"""

def create_recommendation_requests_prompt(posts_text, category, total_posts, time_filter='weekly'):
    """Create prompt for Recommendation Requests category"""
    return f"""You are an entertainment industry analyst creating a comprehensive summary of Reddit entertainment discussions for streaming platforms, content creators, and entertainment enthusiasts.

**CRITICAL: KEEP ENTIRE RESPONSE AROUND 600 WORDS - THIS IS A NOT A HARD LIMIT, COMPLETE FULL SENTENCES AS NEEDED**

**ANALYSIS SCOPE:**
- Category: {category}
- Time Period: {time_filter.title()} data
- Total Posts Analyzed: Top {total_posts} highest-ranked posts
- Focus: Quality over quantity analysis

**CONTENT TO ANALYZE:**
{posts_text}

**REQUIRED OUTPUT STRUCTURE (MAXIMUM 600 WORDS TOTAL):**

**Overview** (40-50 words max)
Write 2 concise sentences capturing the broad themes and most popular content types being discussed.

**Popular Recommendations** (540-550 words max)
Create exactly 10 categories with 2 titles each (20 total recommendations). Start with core genres that appear in discussions (Action, Drama, Horror, Comedy, Romance, Sci-Fi, Anime, TV Shows), then seamlessly continue with mood-based categories based on Reddit patterns.

**Format for each title:**
**Title (Year) | Genre(s) | IMDB Rating, Rotten Tomatoes Rating | Available on [Streaming Platform]**

Structure:
- Create exactly 10 category headers (no section dividers or labels)
- Under each category, list exactly 2 titles using the format above
- Include complete ratings when available (e.g., "8.5/10 IMDB, 95% Rotten Tomatoes")
- If partial ratings appear, include full rating with "/10" or "%" as appropriate
- Be extremely specific about streaming platforms (Netflix, Hulu, Disney+, etc.)
- Flow smoothly from traditional genres to creative mood categories

CRITICAL FORMATTING RULES:
- Use EXACTLY the format: **Title (Year) | Genre(s) | IMDB Rating, Rotten Tomatoes Rating | Available on [Platform]**
- Each category section should have exactly 2 recommendations
- Do NOT use numbered lists (1., 2., etc.) - use the title format only
- Do NOT include priority labels, sub-bullets, or engagement scores
- Always complete ratings fully (8.2/10 IMDB, not just "8.2" or "8.")
- Always complete streaming platforms (Available on Netflix, not just "Available on")
- Be extremely specific about titles, years, ratings, and streaming platforms
- Focus on actionable insights for entertainment discovery
- Each recommendation must be complete - no partial entries or cut-offs
- Provide exactly 10 complete categories with 2 titles each (no more, no less)
"""

def create_reviews_discussions_prompt(posts_text, category, total_posts, time_filter='weekly'):
    """Create prompt for Reviews & Discussions category"""
    return f"""You are an entertainment industry analyst creating a comprehensive summary of Reddit entertainment discussions focusing on user reviews and ongoing discussions.

**CRITICAL: KEEP ENTIRE RESPONSE AROUND 300 WORDS - THIS IS NOT A HARD LIMIT, COMPLETE FULL SECTIONS AS NEEDED**

**ANALYSIS SCOPE:**
- Category: {category}
- Time Period: {time_filter.title()} data
- Total Posts Analyzed: Top {total_posts} highest-ranked posts
- Focus: What users are actually reviewing and discussing

**CONTENT TO ANALYZE:**
{posts_text}

**REQUIRED OUTPUT STRUCTURE (AROUND 300 WORDS TOTAL):**

**Overview** (30-40 words max)
Write 2 concise sentences capturing the main topics being reviewed and discussed by users.

**Popular Reviews** (120-140 words max)
Analyze what content users are actively reviewing. Focus on:
- Movies/shows getting significant discussion
- User sentiment (positive/negative reviews)
- Recurring themes in reviews
- Quality assessments and ratings mentioned

**Trending Discussions** (120-140 words max)
Analyze ongoing conversations and debates. Focus on:
- Hot topics and controversies
- Community debates about entertainment content
- Discussions about industry trends
- Fan theories and analysis

CRITICAL FORMATTING RULES:
- Focus on ACTUAL content being discussed in the posts
- Extract specific titles, opinions, and sentiment from Reddit discussions
- Avoid generic recommendations - analyze real user conversations
- Include specific examples from the discussions when possible
- Highlight community consensus or major disagreements
"""

def create_news_announcements_prompt(posts_text, category, total_posts, time_filter='weekly'):
    """Create prompt for News & Announcements category"""
    return f"""You are an entertainment industry analyst creating a comprehensive summary of Reddit entertainment news and announcements.

**CRITICAL: KEEP ENTIRE RESPONSE AROUND 300 WORDS - THIS IS NOT A HARD LIMIT, COMPLETE FULL SECTIONS AS NEEDED**

**ANALYSIS SCOPE:**
- Category: {category}
- Time Period: {time_filter.title()} data
- Total Posts Analyzed: Top {total_posts} highest-ranked posts
- Focus: Industry news, announcements, and updates

**CONTENT TO ANALYZE:**
{posts_text}

**REQUIRED OUTPUT STRUCTURE (AROUND 300 WORDS TOTAL):**

**Overview** (30-40 words max)
Write 2 concise sentences capturing the major news and announcements being discussed.

**Breaking Entertainment News** (120-140 words max)
Analyze major news stories and announcements. Focus on:
- New project announcements
- Casting news and director assignments
- Release date announcements and delays
- Industry developments and changes
- Award announcements and ceremony news

**Community Reactions** (120-140 words max)
Analyze how the Reddit community is responding to news. Focus on:
- User excitement and anticipation levels
- Concerns and criticisms about announcements
- Predictions and speculation about upcoming content
- Comparisons to previous work or industry trends

CRITICAL FORMATTING RULES:
- Focus on ACTUAL news being discussed in the posts
- Extract specific announcements, dates, and details from Reddit discussions
- Highlight community sentiment about the news
- Include specific examples and user reactions when possible
- Prioritize recent and breaking news over older announcements
"""

def create_lists_rankings_prompt(posts_text, category, total_posts, time_filter='weekly'):
    """Create prompt for Lists & Rankings category"""
    return f"""You are an entertainment industry analyst creating a comprehensive summary of Reddit entertainment lists and community rankings.

**CRITICAL: KEEP ENTIRE RESPONSE AROUND 300 WORDS - THIS IS NOT A HARD LIMIT, COMPLETE FULL SECTIONS AS NEEDED**

**ANALYSIS SCOPE:**
- Category: {category}
- Time Period: {time_filter.title()} data
- Total Posts Analyzed: Top {total_posts} highest-ranked posts
- Focus: User-generated lists, rankings, and top picks

**CONTENT TO ANALYZE:**
{posts_text}

**REQUIRED OUTPUT STRUCTURE (AROUND 300 WORDS TOTAL):**

**Overview** (30-40 words max)
Write 2 concise sentences capturing the types of lists and rankings being shared.

**Popular Community Lists** (120-140 words max)
Analyze user-generated lists and collections. Focus on:
- Most popular list topics and themes
- Frequently mentioned titles across lists
- Creative list categories and criteria
- Community favorites and hidden gems

**Community Rankings & Debates** (120-140 words max)
Analyze ranking discussions and debates. Focus on:
- Popular ranking criteria and methodologies
- Controversial placements and community disagreements
- Consensus picks for "best of" categories
- Emerging trends in community preferences

CRITICAL FORMATTING RULES:
- Focus on ACTUAL lists and rankings being shared in the posts
- Extract specific titles and ranking positions mentioned
- Highlight community consensus and major disagreements
- Include creative list categories and unique ranking approaches
- Prioritize lists with significant community engagement
"""

def create_identification_help_prompt(posts_text, category, total_posts, time_filter='weekly'):
    """Create prompt for Identification & Help category"""
    return f"""You are an entertainment industry analyst creating a comprehensive summary of Reddit entertainment identification requests and help threads.

**CRITICAL: KEEP ENTIRE RESPONSE AROUND 300 WORDS - THIS IS NOT A HARD LIMIT, COMPLETE FULL SECTIONS AS NEEDED**

**ANALYSIS SCOPE:**
- Category: {category}
- Time Period: {time_filter.title()} data
- Total Posts Analyzed: Top {total_posts} highest-ranked posts
- Focus: Help requests and community assistance

**CONTENT TO ANALYZE:**
{posts_text}

**REQUIRED OUTPUT STRUCTURE (AROUND 300 WORDS TOTAL):**

**Overview** (30-40 words max)
Write 2 concise sentences capturing the main types of help requests and identification needs.

**Common Identification Requests** (120-140 words max)
Analyze what users are trying to identify. Focus on:
- Most common types of identification requests
- Successful identifications and solutions provided
- Recurring themes in "tip of my tongue" posts
- Popular content being rediscovered

**Community Help & Solutions** (120-140 words max)
Analyze how the community provides assistance. Focus on:
- Most helpful community responses and resources
- Creative problem-solving approaches
- Tools and methods shared for identification
- Success stories and grateful users

CRITICAL FORMATTING RULES:
- Focus on ACTUAL help requests and solutions from the posts
- Extract specific identification challenges and successful answers
- Highlight effective community problem-solving methods
- Include examples of difficult identifications that were solved
- Prioritize requests with strong community engagement and solutions
"""

def create_entertainment_prompt(posts_text, category, total_posts, time_filter='weekly'):
    """Create entertainment-focused prompt for AI summarization with category-specific routing"""
    
    # Route to category-specific prompts
    if category == 'Recommendation Requests':
        return create_recommendation_requests_prompt(posts_text, category, total_posts, time_filter)
    elif category == 'Reviews & Discussions':
        return create_reviews_discussions_prompt(posts_text, category, total_posts, time_filter)
    elif category == 'News & Announcements':
        return create_news_announcements_prompt(posts_text, category, total_posts, time_filter)
    elif category == 'Lists & Rankings':
        return create_lists_rankings_prompt(posts_text, category, total_posts, time_filter)
    elif category == 'Identification & Help':
        return create_identification_help_prompt(posts_text, category, total_posts, time_filter)
    else:
        # Default to recommendation requests for unknown categories
        return create_recommendation_requests_prompt(posts_text, category, total_posts, time_filter)