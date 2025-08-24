#!/bin/bash
# Script to run coverage tests and generate reports

echo "🧪 Running tests with coverage..."
uv run pytest tests/ --ignore=tests/integration/ --cov=src --cov-report=html --cov-report=term --cov-report=xml -v

echo ""
echo "📊 Coverage reports generated:"
echo "  - HTML: coverage_html/index.html"
echo "  - XML: coverage.xml"
echo "  - Terminal: displayed above"

if command -v open &> /dev/null; then
    echo ""
    echo "🌐 Opening HTML coverage report..."
    open coverage_html/index.html
elif command -v xdg-open &> /dev/null; then
    echo ""
    echo "🌐 Opening HTML coverage report..."
    xdg-open coverage_html/index.html
else
    echo ""
    echo "💡 To view the HTML report, open coverage_html/index.html in your browser"
fi
