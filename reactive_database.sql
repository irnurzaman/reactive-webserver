--
-- PostgreSQL database dump
--

-- Dumped from database version 12.2 (Ubuntu 12.2-4)
-- Dumped by pg_dump version 12rc1

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

--
-- Name: reactive; Type: DATABASE; Schema: -; Owner: bahana_olt
--

CREATE DATABASE reactive WITH TEMPLATE = template0 ENCODING = 'UTF8' LC_COLLATE = 'en_US.UTF-8' LC_CTYPE = 'en_US.UTF-8';


ALTER DATABASE reactive OWNER TO bahana_olt;

\connect reactive

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

--
-- Name: pgcrypto; Type: EXTENSION; Schema: -; Owner: -
--

CREATE EXTENSION IF NOT EXISTS pgcrypto WITH SCHEMA public;


--
-- Name: EXTENSION pgcrypto; Type: COMMENT; Schema: -; Owner: 
--

COMMENT ON EXTENSION pgcrypto IS 'cryptographic functions';


SET default_tablespace = '';

SET default_table_access_method = heap;

--
-- Name: accounts; Type: TABLE; Schema: public; Owner: reactive
--

CREATE TABLE public.accounts (
    account_no character varying NOT NULL,
    balance bigint DEFAULT 0 NOT NULL,
    balance_hold bigint DEFAULT 0 NOT NULL
);


ALTER TABLE public.accounts OWNER TO reactive;

--
-- Name: orders; Type: TABLE; Schema: public; Owner: bahana_olt
--

CREATE TABLE public.orders (
    account character varying(6) NOT NULL,
    action character varying(4) NOT NULL,
    stock character varying(4) NOT NULL,
    price integer NOT NULL,
    vol bigint NOT NULL,
    "timestamp" timestamp without time zone DEFAULT CURRENT_TIMESTAMP NOT NULL,
    order_id character varying(8) NOT NULL
);


ALTER TABLE public.orders OWNER TO bahana_olt;

--
-- Name: users; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.users (
    username character varying NOT NULL,
    password character varying NOT NULL
);


ALTER TABLE public.users OWNER TO postgres;

--
-- Name: accounts accounts_pkey; Type: CONSTRAINT; Schema: public; Owner: reactive
--

ALTER TABLE ONLY public.accounts
    ADD CONSTRAINT accounts_pkey PRIMARY KEY (account_no);


--
-- Name: accounts unique_accounts_account_no; Type: CONSTRAINT; Schema: public; Owner: reactive
--

ALTER TABLE ONLY public.accounts
    ADD CONSTRAINT unique_accounts_account_no UNIQUE (account_no);


--
-- PostgreSQL database dump complete
--

