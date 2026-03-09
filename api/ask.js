const handleQuery = async (q) => {
  const questionText = q || query;
  if (!questionText.trim()) return;

  setLoading(true);
  setError('');
  setResponse('');
  setAsked(true);

  const dataContext = buildContext();

  try {
    const res = await fetch('/api/ask', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        question: questionText,
        dataContext
      })
    });

    const data = await res.json();

    if (!res.ok) {
      throw new Error(data?.error || 'Request failed');
    }

    if (!data?.text) {
      throw new Error('Empty response');
    }

    setResponse(data.text);
    if (q) setQuery(q);
  } catch (e) {
    setError('Unable to get a response. Please try again.');
  } finally {
    setLoading(false);
  }
};
