-- phpMyAdmin SQL Dump
-- version 5.2.0
-- https://www.phpmyadmin.net/
--
-- Host: 127.0.0.1
-- Generation Time: Nov 24, 2022 at 03:07 PM
-- Server version: 10.4.25-MariaDB
-- PHP Version: 7.4.30

SET SQL_MODE = "NO_AUTO_VALUE_ON_ZERO";
START TRANSACTION;
SET time_zone = "+00:00";


/*!40101 SET @OLD_CHARACTER_SET_CLIENT=@@CHARACTER_SET_CLIENT */;
/*!40101 SET @OLD_CHARACTER_SET_RESULTS=@@CHARACTER_SET_RESULTS */;
/*!40101 SET @OLD_COLLATION_CONNECTION=@@COLLATION_CONNECTION */;
/*!40101 SET NAMES utf8mb4 */;

--
-- Database: `kafkaevendb`
--

-- --------------------------------------------------------

--
-- Table structure for table `evennumbers`
--

CREATE TABLE `evennumbers` (
  `id` int(11) NOT NULL,
  `evennumbers` int(11) NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

--
-- Dumping data for table `evennumbers`
--

INSERT INTO `evennumbers` (`id`, `evennumbers`) VALUES
(1, 36),
(2, 502),
(3, 476),
(4, 334),
(5, 770),
(6, 644),
(7, 460),
(8, 184),
(9, 52),
(10, 232),
(11, 340),
(12, 484),
(13, 784),
(14, 46),
(15, 714),
(16, 516),
(17, 640),
(18, 542),
(19, 136),
(20, 710),
(21, 522),
(22, 832),
(23, 894),
(24, 248),
(25, 274),
(26, 272),
(27, 524),
(28, 130),
(29, 980),
(30, 252),
(31, 88),
(32, 670),
(33, 920),
(34, 534),
(35, 412),
(36, 522),
(37, 308),
(38, 610),
(39, 624),
(40, 950),
(41, 828),
(42, 746),
(43, 512),
(44, 616),
(45, 46),
(46, 264),
(47, 646),
(48, 332),
(49, 420),
(50, 446),
(51, 870),
(52, 732),
(53, 930),
(54, 964),
(55, 544),
(56, 350),
(57, 296),
(58, 164),
(59, 732),
(60, 732),
(61, 140),
(62, 692),
(63, 904),
(64, 724),
(65, 522),
(66, 608),
(67, 880),
(68, 842),
(69, 590),
(70, 54),
(71, 702),
(72, 326),
(73, 914),
(74, 844),
(75, 920),
(76, 334),
(77, 658),
(78, 936),
(79, 172),
(80, 540),
(81, 612),
(82, 492),
(83, 900),
(84, 106),
(85, 230),
(86, 748),
(87, 532),
(88, 210),
(89, 896),
(90, 78),
(91, 404),
(92, 738),
(93, 396),
(94, 812),
(95, 118),
(96, 114),
(97, 134),
(98, 376);

-- --------------------------------------------------------

--
-- Table structure for table `reversenumbers`
--

CREATE TABLE `reversenumbers` (
  `id` int(11) NOT NULL,
  `reverseNumbers` int(11) NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

--
-- Dumping data for table `reversenumbers`
--

INSERT INTO `reversenumbers` (`id`, `reverseNumbers`) VALUES
(1, 565),
(2, 443),
(3, 794),
(4, 969),
(5, 69),
(6, 86),
(7, 816),
(8, 494),
(9, 565),
(10, 443),
(11, 794),
(12, 969),
(13, 69),
(14, 86),
(15, 816),
(16, 494),
(17, 978),
(18, 606),
(19, 638),
(20, 428),
(21, 548),
(22, 945),
(23, 75),
(24, 204),
(25, 803),
(26, 937),
(27, 196),
(28, 72);

-- --------------------------------------------------------

--
-- Table structure for table `wordcount`
--

CREATE TABLE `wordcount` (
  `id` int(11) NOT NULL,
  `message` varchar(50) NOT NULL,
  `count` int(11) NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

--
-- Dumping data for table `wordcount`
--

INSERT INTO `wordcount` (`id`, `message`, `count`) VALUES
(1, 'india is my country', 19);

--
-- Indexes for dumped tables
--

--
-- Indexes for table `evennumbers`
--
ALTER TABLE `evennumbers`
  ADD PRIMARY KEY (`id`);

--
-- Indexes for table `reversenumbers`
--
ALTER TABLE `reversenumbers`
  ADD PRIMARY KEY (`id`);

--
-- Indexes for table `wordcount`
--
ALTER TABLE `wordcount`
  ADD PRIMARY KEY (`id`);

--
-- AUTO_INCREMENT for dumped tables
--

--
-- AUTO_INCREMENT for table `evennumbers`
--
ALTER TABLE `evennumbers`
  MODIFY `id` int(11) NOT NULL AUTO_INCREMENT, AUTO_INCREMENT=99;

--
-- AUTO_INCREMENT for table `reversenumbers`
--
ALTER TABLE `reversenumbers`
  MODIFY `id` int(11) NOT NULL AUTO_INCREMENT, AUTO_INCREMENT=29;

--
-- AUTO_INCREMENT for table `wordcount`
--
ALTER TABLE `wordcount`
  MODIFY `id` int(11) NOT NULL AUTO_INCREMENT, AUTO_INCREMENT=2;
COMMIT;

/*!40101 SET CHARACTER_SET_CLIENT=@OLD_CHARACTER_SET_CLIENT */;
/*!40101 SET CHARACTER_SET_RESULTS=@OLD_CHARACTER_SET_RESULTS */;
/*!40101 SET COLLATION_CONNECTION=@OLD_COLLATION_CONNECTION */;
