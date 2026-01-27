--
-- PostgreSQL database dump
--

\restrict gG4ujlhTBPhK8tyNVH9FhD3GQXE08yB9ErQ0D6PaRCxuMYLshmqCHEKIvFDoOmz

-- Dumped from database version 14.20 (Debian 14.20-1.pgdg13+1)
-- Dumped by pg_dump version 16.10 (Ubuntu 16.10-0ubuntu0.24.04.1)

SET statement_timeout = 0;
SET lock_timeout = 0;
SET idle_in_transaction_session_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SELECT pg_catalog.set_config('search_path', '', false);
SET check_function_bodies = false;
SET xmloption = content;
SET client_min_messages = warning;
SET row_security = off;

SET default_tablespace = '';

SET default_table_access_method = heap;

--
-- Name: datatypes_test_table; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.datatypes_test_table (
    id integer NOT NULL,
    int8_val smallint,
    int16_val smallint,
    int32_val integer,
    int64_val bigint,
    uint8_val smallint,
    uint16_val integer,
    uint32_val bigint,
    uint64_val bigint,
    float32_val real,
    float64_val double precision,
    bool_val boolean,
    string_val text,
    date_val date,
    timestamp_val timestamp without time zone
);


ALTER TABLE public.datatypes_test_table OWNER TO postgres;

--
-- Name: datatypes_test_table_id_seq; Type: SEQUENCE; Schema: public; Owner: postgres
--

CREATE SEQUENCE public.datatypes_test_table_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE public.datatypes_test_table_id_seq OWNER TO postgres;

--
-- Name: datatypes_test_table_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: postgres
--

ALTER SEQUENCE public.datatypes_test_table_id_seq OWNED BY public.datatypes_test_table.id;


--
-- Name: datatypes_test_table id; Type: DEFAULT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.datatypes_test_table ALTER COLUMN id SET DEFAULT nextval('public.datatypes_test_table_id_seq'::regclass);


--
-- Data for Name: datatypes_test_table; Type: TABLE DATA; Schema: public; Owner: postgres
--

COPY public.datatypes_test_table (id, int8_val, int16_val, int32_val, int64_val, uint8_val, uint16_val, uint32_val, uint64_val, float32_val, float64_val, bool_val, string_val, date_val, timestamp_val) FROM stdin;
1	127	32767	2147483647	9223372036854775807	255	65535	2147483647	9223372036854775807	3.14	2.718281828	t	Test String 1	2024-01-15	2024-01-15 10:30:00
2	-128	-32768	-2147483648	-9223372036854775808	0	0	0	0	-1.5	-999.123	f	Test String 2	2023-12-25	2023-12-25 23:59:59
3	0	0	0	0	128	32768	1073741824	4611686018427387904	0	0	t	Test String 3	2024-06-30	2024-06-30 12:00:00
\.


--
-- Name: datatypes_test_table_id_seq; Type: SEQUENCE SET; Schema: public; Owner: postgres
--

SELECT pg_catalog.setval('public.datatypes_test_table_id_seq', 3, true);


--
-- Name: datatypes_test_table datatypes_test_table_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.datatypes_test_table
    ADD CONSTRAINT datatypes_test_table_pkey PRIMARY KEY (id);


--
-- PostgreSQL database dump complete
--

\unrestrict gG4ujlhTBPhK8tyNVH9FhD3GQXE08yB9ErQ0D6PaRCxuMYLshmqCHEKIvFDoOmz

