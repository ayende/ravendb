using System;
using Raven.Studio.Infrastructure;

namespace Raven.Studio.Models
{
    public enum DocumentDisplayStyle
    {
        Details,
        Card,
        IdOnly
    }

	public class DocumentSize : NotifyPropertyChangedBase
	{
		public const double CardMinimumHeight = 130;
	    public const double CardMaximumHeight = 700;

	    private double indicatorPosition;
	    private DocumentDisplayStyle displayStyle;
        private double height;
        private double width;

		private readonly static DocumentSize current = new DocumentSize()
		                                                   {
		                                                       IndicatorPosition =  Settings.Instance.DocumentSize
		                                                   };
		public static DocumentSize Current
		{
			get { return current; }
		}

		public event EventHandler SizeChanged;

	    public readonly double MinimumIndicatorPosition = 0;
	    public readonly double MaximumIndicatorPosition = 100;
	    private const double DetailsToIdSwitchover = 20;
	    private const double IdToCardSwitchover = 40;
	    private const string IndicatorPositionSettingsKey = "DocumentSize.IndicatorPosition";

	    public DocumentSize()
        {
        }

	    public double IndicatorPosition
	    {
	        get { return indicatorPosition; }
            set
            {
                if (indicatorPosition == value)
                {
                    return;
                }

                indicatorPosition = value;

                if (indicatorPosition < DetailsToIdSwitchover / 2)
                    indicatorPosition = 0;
                else if (indicatorPosition < (IdToCardSwitchover - (IdToCardSwitchover - DetailsToIdSwitchover) / 2))
                    indicatorPosition = DetailsToIdSwitchover;
                else if (indicatorPosition < IdToCardSwitchover)
                    indicatorPosition = IdToCardSwitchover;

                Settings.Instance.DocumentSize = (int)IndicatorPosition;

                UpdateHeightWidthAndDisplayStyle();
                OnPropertyChanged(() => IndicatorPosition);
            }
	    }

	    private void UpdateHeightWidthAndDisplayStyle()
	    {
            if (indicatorPosition < DetailsToIdSwitchover)
	        {
	            DisplayStyle = DocumentDisplayStyle.Details;
	        }
            else if (indicatorPosition < IdToCardSwitchover)
	        {
	            DisplayStyle = DocumentDisplayStyle.IdOnly;
	            Height = 85;
	            Width = 204;
	        }
            else
            {
                DisplayStyle = DocumentDisplayStyle.Card;
                var cardScale = (indicatorPosition - IdToCardSwitchover) / (MaximumIndicatorPosition - IdToCardSwitchover);
                Height = CardMinimumHeight + (CardMaximumHeight - CardMinimumHeight)*cardScale;
            }
	    }

	    public double Height
		{
			get { return height; }
			set
			{
				if (height == value)
					return;
				height = value;
				OnPropertyChanged(() => Height);
				SetWidthBasedOnHeight();
			}
		}

		public double Width
		{
			get { return width; }
			private set
			{
				width = value;
				OnPropertyChanged(() => Width);
			}
		}

        public DocumentDisplayStyle DisplayStyle
        {
            get { return displayStyle; }
            private set
            {
                if (displayStyle != value)
                {
                    displayStyle = value;
                    OnPropertyChanged(() => DisplayStyle);
                }
            }
        }

		private void SetWidthBasedOnHeight()
		{
			const double wideAspectRatio = 1.7;
			const double narrowAspectRatio = 0.707;
			const double aspectRatioSwitchoverHeight = 120;
			const double wideRatioMaxWidth = aspectRatioSwitchoverHeight*wideAspectRatio;
			const double narrowAspectRatioSwitchoverHeight = wideRatioMaxWidth/narrowAspectRatio;

			Width = Height < aspectRatioSwitchoverHeight ? Height*wideAspectRatio
			        	: Height < narrowAspectRatioSwitchoverHeight ? wideRatioMaxWidth
			        	  	: Height*narrowAspectRatio;

			if (SizeChanged != null)
				SizeChanged(this, EventArgs.Empty);
		}
	}
}