import streamlit as st

pages =[
    st.Page("pages/1_home.py", title="Home"),
    st.Page("pages/2_basis.py", title="Basis"),
    st.Page("pages/3_diff_base.py", title="Diferencial de base"),
    st.Page("pages/4_rel_prices.py", title="Preços relativos"),
    st.Page("pages/5_calendar_spreads.py", title="Calendar Spreads"),
    st.Page("pages/6_Ratios.py", title="Ratios "),
]

pg = st.navigation(pages, position="top")
pg.run()