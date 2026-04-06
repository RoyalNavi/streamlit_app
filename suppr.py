import yfinance as yf

import yfinance as yf
import matplotlib.pyplot as plt
import plotly.graph_objects as go


ticker = 'AAPL'
stock = yf.Ticker(ticker)
data = stock.history(period='5d', interval='5m')

# Création du graphe interactif
fig = go.Figure()

fig.add_trace(go.Scatter(
    x=data.index,
    y=data["Open"],
    mode="lines",
    name=f"Prix d'ouverture {ticker}",
    line=dict(color="royalblue"),
    hovertemplate="Date : %{x}<br>Prix : %{y:.2f} USD<extra></extra>"
))

fig.update_layout(
    title=f"📈 {ticker}",
    xaxis_title="Date",
    yaxis_title="Prix (USD)",
    template="plotly_white",
    hovermode="x unified",
    margin=dict(l=40, r=40, t=60, b=40)
)

fig.show()